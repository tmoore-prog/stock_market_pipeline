from bigquery_client import BigQueryManager
from pendulum import parse

bq_manager = BigQueryManager()


def load_data(df, date_str, run_id):
    if df is not None and not df.empty:
        bq_manager.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status='started',
            total_tickers=len(df['T'].unique()) if 'T' in df.columns else 0
        )

        success, rows_inserted = bq_manager.insert_stock_data(df, date_str)

        if success:
            bq_manager.record_checkpoint(
                run_id=run_id,
                api_date=date_str,
                status='completed',
                total_tickers=len(df['T'].unique()) if 'T' in df.columns else 0,
                rows_inserted=rows_inserted
            )
            print(f"Successfully saved {rows_inserted} records for {date_str}")
        else:
            bq_manager.record_checkpoint(
                run_id=run_id,
                api_date=date_str,
                status='failed',
                error_message="Failed to insert data to BiqQuery"
            )
            print(f"Failed to save data for {date_str}")
    else:
        print(f"No data to save for {date_str}")
