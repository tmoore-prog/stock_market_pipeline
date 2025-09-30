from google.cloud import bigquery
import pandas as pd
import pendulum
from config import (
    BIGQUERY_DATASET,
    BIGQUERY_TABLE,
    CHECKPOINT_TABLE,
    GCP_PROJECT_ID,
    credentials
)


class BigQueryManager:
    def __init__(self):
        '''Initialize bigquery client and ensure tables exist'''
        self.client = bigquery.Client(
            project=GCP_PROJECT_ID,
            credentials=credentials
        )
        self.dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"
        self.table_id = f"{self.dataset_id}.{BIGQUERY_TABLE}"
        self.checkpoint_table_id = f"{self.dataset_id}.{CHECKPOINT_TABLE}"

        # Ensure that the dataset and tables exist with class methods
        self._ensure_dataset_exists()
        self._ensure_tables_exist()

    def _ensure_dataset_exists(self):
        # Create dataset if it doesn't exist
        dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = 'US'

        try:
            self.client.get_dataset(dataset_id)
            print(f"Dataset {BIGQUERY_DATASET} already exists!")
        except Exception:
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {BIGQUERY_DATASET}")

    def _ensure_tables_exist(self):
        # Create tables if they don't exist
        stock_schema = [
            bigquery.SchemaField("T", "STRING", description="Ticker symbol"),
            bigquery.SchemaField("v", "FLOAT", description="Volume"),
            bigquery.SchemaField("vw", "FLOAT", description="Volume weighted average"),
            bigquery.SchemaField("o", "FLOAT", description="Open price"),
            bigquery.SchemaField("c", "FLOAT", description="Close price"),
            bigquery.SchemaField("h", "FLOAT", description="High price"),
            bigquery.SchemaField("l", "FLOAT", description="Low price"),
            bigquery.SchemaField("ts", "TIMESTAMP", description="Timestamp"),
            bigquery.SchemaField("n", "INTEGER", description="Number of transactions"),
            bigquery.SchemaField("date", "DATE", description="Trade date"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", description="Time record was loaded")
        ]

        checkpoint_table_schema = [
            bigquery.SchemaField("run_id", "STRING", description="Unique run identifier"),
            bigquery.SchemaField("api_date", "DATE", description="Date requested from API"),
            bigquery.SchemaField("status", "STRING", description="Status: started, completed, failed"),
            bigquery.SchemaField("total_tickers", "INTEGER", description="Number of tickers returned"),
            bigquery.SchemaField("rows_inserted", "INTEGER", description="Rows successfully inserted"),
            bigquery.SchemaField("started_at", "TIMESTAMP", description="When processing started"),
            bigquery.SchemaField("completed_at", "TIMESTAMP", description="When processing completed"),
            bigquery.SchemaField("error_message", "STRING", description="Error details if failed")
        ]

        self._create_table_if_not_exists(
            self.table_id,
            stock_schema,
            partition_field="date",
            clustering_fields=["T"]
        )

        self._create_table_if_not_exists(
            self.checkpoint_table_id,
            checkpoint_table_schema
        )

    def _create_table_if_not_exists(self, table_id, schema,
                                    partition_field=None,
                                    clustering_fields=None):
        try:
            self.client.get_table(table_id)
            print(f"Table {table_id} already exists")
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            if clustering_fields:
                table.clustering_fields = clustering_fields

            table = self.client.create_table(table)
            print(f"Created table {table_id}")

    def insert_stock_data(self, df, date_str):
        '''Insert stock data to BigQuery'''
        if df is None or df.empty:
            return False, 0

        # Add metadata
        df = df.rename(columns={
            't': 'ts'
        })
        df['date'] = pd.to_datetime(date_str).date()
        df['ingested_at'] = pendulum.now()

        # Transform timestamp into proper form
        if 'ts' in df.columns:
            df['ts'] = pd.to_datetime(df['ts'], unit='ms')

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

        try:
            job = self.client.load_table_from_dataframe(
                df,
                self.table_id,
                job_config=job_config
            )
            job.result()

            rows_inserted = len(df)
            print(f"Inserted {rows_inserted} rows for {date_str}")
            return True, rows_inserted
        except Exception as e:
            print(f"Failed to insert data for {date_str}: {e}")
            return False, 0

    def record_checkpoint(self, run_id, api_date, status, total_tickers=None,
                          rows_inserted=None, error_message=None):
        '''Record checkpoint information'''
        checkpoint_data = [{
            'run_id': run_id,
            'api_date': api_date,
            'status': status,
            'total_tickers': total_tickers,
            'rows_inserted': rows_inserted,
            'started_at': pendulum.now() if status == 'started' else None,
            'completed_at': pendulum.now() if status in ['completed',
                                                         'failed'] else None,
            'error_message': error_message
        }]

        df = pd.DataFrame(checkpoint_data)

        # For 'started' status we insert, else update
        if status == 'started':
            job = self.client.load_table_from_dataframe(
                df,
                self.checkpoint_table_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            )
        else:
            # Update existing record
            query = f"""
            UPDATE `{self.checkpoint_table_id}`
            SET status = @status,
                total_tickers = @total_tickers,
                rows_inserted = @rows_inserted,
                completed_at = CURRENT_TIMESTAMP(),
                error_message = @error_message\
            WHERE run_id = @run_id AND api_date = @api_date
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("total_tickers", "INTEGER", total_tickers),
                    bigquery.ScalarQueryParameter("rows_inserted", "INTEGER", rows_inserted),
                    bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                    bigquery.ScalarQueryParameter("api_date", "DATE", api_date)
                ]
            )

            job = self.client.query(query, job_config=job_config)

        job.result()
        print(f"Checkpoint recorded {api_date} - {status}")

    def get_completed_dates(self):
        '''Query checkpoint table to get list of successfully completed dates'''
        query = f"""
        SELECT DISTINCT api_date
        FROM `{self.checkpoint_table_id}`
        WHERE status = 'completed'
        ORDER BY api_date
        """

        try:
            results = self.client.query(query).result()
            completed_dates = {row.api_date.strftime('%Y-%m-%d') for row in results}
            print(f"Found {len(completed_dates)} completed dates in checkpoint table")
            return completed_dates
        except Exception as e:
            print(f"Error reading checkpoint table: {e}")
            print("Starting fresh with no completed dates")
            return set()

    def get_ingestion_stats(self):
        '''Get ingestion statistics for monitoring'''
        query = f"""
        SELECT
            COUNT(DISTINCT api_date) as days_processed,
            SUM(rows_inserted) as total_rows,
            AVG(total_tickers) as avg_tickers_per_day,
            MIN(api_date) as earliest_date,
            MAX(api_date) as latest_date,
            COUNTIF(status = 'failed') as failed_runs
        FROM `{self.checkpoint_table_id}`
        WHERE status = 'completed'
        """

        try:
            results = list(self.client.query(query).result())[0]
            return {
                'days_processed': results.days_processed,
                'total_rows': results.total_rows,
                'avg_tickers_per_day': results.avg_tickers_per_day,
                'earliest_date': results.earliest_date,
                'latest_date': results.latest_date,
                'failed_runs': results.failed_runs
            }
        except Exception:
            return None
