
import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'rinatmubinov'
cohort = '1'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    df = df.drop('id', axis=1)
    df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'
        print("Added default 'shipped' status for backward compatibility")
    
    valid_statuses = ['shipped', 'refunded']
    df['status'] = df['status'].fillna('shipped')
    df = df[df['status'].isin(valid_statuses)]
    
    status_counts = df['status'].value_counts()
    print(f"Status distribution: {status_counts.to_dict()}")
    
    if 'refunded' in df['status'].values:
        refunded_revenue = df[df['status'] == 'refunded']['price'].sum()
        print(f"Refunded revenue impact: {refunded_revenue}")

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


def calculate_customer_retention_metrics(ti):
    print('Calculating customer retention metrics')
    
    # Получаем текущую дату выполнения
    execution_date = ti.execution_date
    week_start = execution_date - timedelta(days=execution_date.weekday())
    week_end = week_start + timedelta(days=6)
    week_number = week_start.isocalendar()[1]
    
    print(f"Calculating retention for week {week_number}: {week_start.date()} to {week_end.date()}")
    
    postgres_hook = PostgresHook(postgres_conn_id)
    
    query = """
    WITH weekly_orders AS (
        SELECT 
            customer_id,
            product_id,
            price,
            quantity,
            status,
            date_time
        FROM mart.f_sales
        WHERE date_time >= %s 
        AND date_time <= %s
    ),
    customer_stats AS (
        SELECT 
            customer_id,
            COUNT(DISTINCT date_time::date) as order_days,
            SUM(CASE WHEN status = 'shipped' THEN price * quantity ELSE 0 END) as revenue,
            SUM(CASE WHEN status = 'refunded' THEN price * quantity ELSE 0 END) as refunded_amount,
            COUNT(CASE WHEN status = 'refunded' THEN 1 END) as refund_count
        FROM weekly_orders
        GROUP BY customer_id
    ),
    customer_categories AS (
        SELECT 
            customer_id,
            CASE 
                WHEN order_days = 1 AND refund_count = 0 THEN 'new'
                WHEN order_days > 1 AND refund_count = 0 THEN 'returning'
                WHEN refund_count > 0 THEN 'refunded'
                ELSE 'new'
            END as customer_type,
            revenue,
            refunded_amount,
            refund_count
        FROM customer_stats
    ),
    product_customer_stats AS (
        SELECT 
            wo.product_id,
            cc.customer_type,
            COUNT(DISTINCT wo.customer_id) as customer_count,
            SUM(CASE WHEN wo.status = 'shipped' THEN wo.price * wo.quantity ELSE 0 END) as revenue,
            COUNT(CASE WHEN wo.status = 'refunded' THEN 1 END) as refund_count
        FROM weekly_orders wo
        JOIN customer_categories cc ON wo.customer_id = cc.customer_id
        GROUP BY wo.product_id, cc.customer_type
    )
    SELECT 
        product_id,
        customer_type,
        customer_count,
        revenue,
        refund_count
    FROM product_customer_stats
    """
    
    df = postgres_hook.get_pandas_df(query, parameters=[week_start, week_end])
    
    retention_data = []
    
    for product_id in df['product_id'].unique():
        product_df = df[df['product_id'] == product_id]
        
        new_customers_count = 0
        returning_customers_count = 0
        refunded_customer_count = 0
        new_customers_revenue = 0
        returning_customers_revenue = 0
        customers_refunded = 0
        
        for _, row in product_df.iterrows():
            if row['customer_type'] == 'new':
                new_customers_count = row['customer_count']
                new_customers_revenue = row['revenue']
            elif row['customer_type'] == 'returning':
                returning_customers_count = row['customer_count']
                returning_customers_revenue = row['revenue']
            elif row['customer_type'] == 'refunded':
                refunded_customer_count = row['customer_count']
                customers_refunded = row['refund_count']
        
        retention_data.append({
            'new_customers_count': new_customers_count,
            'returning_customers_count': returning_customers_count,
            'refunded_customer_count': refunded_customer_count,
            'period_name': 'weekly',
            'period_id': week_number,
            'item_id': product_id,
            'new_customers_revenue': new_customers_revenue,
            'returning_customers_revenue': returning_customers_revenue,
            'customers_refunded': customers_refunded
        })
    
    print(f"Calculated retention metrics for {len(retention_data)} products")
    ti.xcom_push(key='retention_data', value=retention_data)


def load_customer_retention_data(ti):
    print('Loading customer retention data')
    
    retention_data = ti.xcom_pull(key='retention_data')
    execution_date = ti.execution_date
    week_number = (execution_date - timedelta(days=execution_date.weekday())).isocalendar()[1]
    
    postgres_hook = PostgresHook(postgres_conn_id)
    
    delete_sql = """
    DELETE FROM mart.f_customer_retention 
    WHERE period_name = 'weekly' AND period_id = %s
    """
    postgres_hook.run(delete_sql, parameters=[week_number])
    print(f"Cleared existing data for week {week_number}")
    for record in retention_data:
        insert_sql = """
        INSERT INTO mart.f_customer_retention (
            new_customers_count,
            returning_customers_count,
            refunded_customer_count,
            period_name,
            period_id,
            item_id,
            new_customers_revenue,
            returning_customers_revenue,
            customers_refunded
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        postgres_hook.run(insert_sql, parameters=(
            record['new_customers_count'],
            record['returning_customers_count'],
            record['refunded_customer_count'],
            record['period_name'],
            record['period_id'],
            record['item_id'],
            record['new_customers_revenue'],
            record['returning_customers_revenue'],
            record['customers_refunded']
        ))
    
    print(f"Loaded {len(retention_data)} retention records")


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'sprint3_enhanced',
        default_args=args,
        description='Enhanced sprint3 with refund support and customer retention',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": business_dt}
    )
    
    calculate_retention = PythonOperator(
        task_id='calculate_customer_retention',
        python_callable=calculate_customer_retention_metrics)
    
    load_retention = PythonOperator(
        task_id='load_customer_retention',
        python_callable=load_customer_retention_data)

    (
        generate_report
        >> get_report
        >> get_increment
        >> upload_user_order_inc
        >> [update_d_item_table, update_d_city_table, update_d_customer_table]
        >> update_f_sales
        >> calculate_retention
        >> load_retention
    )
