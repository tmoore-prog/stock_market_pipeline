from airflow.decorators import dag, task
from pendulum import timezone, datetime


@dag(
    schedule='0 12 * * 2-6',
    dag_id='market_data_pipeline',
    start_date=datetime(2025, 8, 1, tz=(timezone('America/New_York'))),
    catchup=False,
    tags=['etl', 'daily']
)
def market_data_pipeline():
    # Nice documentation

    @task()
    def extract():
        from extract_load_polygon_data import extract_load_data
        return extract_load_data()

    @task.bash
    def run_dbt_staging():
        return '''cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select staging --profiles-dir .
        '''

    @task.bash
    def run_dbt_intermediate():
        return '''cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select intermediate --profiles-dir .
        '''

    @task.bash
    def run_dbt_marts():
        return '''cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select marts --profiles-dir .
        '''

    @task.bash
    def run_dbt_tests():
        return '''cd /opt/airflow/dbt/stock_analytics && \
            dbt test --profiles-dir .
        '''

    extract() >> run_dbt_staging() >> run_dbt_intermediate() \
        >> run_dbt_marts() >> run_dbt_tests()


market_data_pipeline()
