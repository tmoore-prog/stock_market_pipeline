# Simple program to extract a preliminary time frame of market data
from utils import get_trading_days, get_completed_dates
from extraction import extract_polygon_data
from load import load_data
import time
import pendulum
from pendulum import duration


def extract_load_data(years_back=2, days_back_override=None):
    run_id = pendulum.now().strftime('%Y%m%d_%H%M%S')
    print(f"Starting historical data load with run_id: {run_id}")

    end_date = pendulum.now().date() - duration(days=1)

    if days_back_override:
        start_date = end_date - duration(days=days_back_override)
    else:
        start_date = end_date - duration(years=years_back)
    completed_dates = get_completed_dates()
    trading_days = get_trading_days(start_date, end_date)
    total_days = len(trading_days)
    remaining_days = len([d for d in trading_days if d.strftime('%Y-%m-%d')
                          not in completed_dates])

    print(f"Total trading days: {total_days}")
    print(f"Already completed: {len(completed_dates)}")
    print(f"Remaining to process: {remaining_days}")

    for i, date in enumerate(trading_days, 1):
        date_str = date.strftime('%Y-%m-%d')
        if date_str in completed_dates:
            print(f"Skipping {date_str} (already completed). "
                  f"Progress: {i}/{total_days}")
            continue

        print(f"Processing {date_str}. Progress {i}/{total_days} "
              f"(Remaining: {remaining_days})")

        df = extract_polygon_data(date_str)

        load_data(df, date_str, run_id)

        time.sleep(20)

        remaining_days -= 1

    print(f"Finished processing {total_days} trading days.")

    from bigquery_client import BigQueryManager
    bq_manager = BigQueryManager()
    stats = bq_manager.get_ingestion_stats()
    if stats:
        print("\n=== Ingestion Summary ===")
        print(f"Days processed: {stats['days_processed']}")
        print(f"Total rows: {stats['total_rows']:,}")
        print(f"Average tickers per day: {stats['avg_tickers_per_day']:,.0f}")
        print(f"Date range: {stats['earliest_date']} to "
              f"{stats['latest_date']}")


if __name__ == "__main__":
    extract_load_data()
