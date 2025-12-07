import time
import subprocess
import sys


def run_processing():
    print("=" * 60)
    print("RUNNING DATA PROCESSING PIPELINE")
    print("=" * 60)

    print("\n[1/2] Processing Silver Layer...")
    subprocess.run([sys.executable, 'processing/silver_layer.py'])

    time.sleep(2)

    print("\n[2/2] Processing Gold Layer...")
    subprocess.run([sys.executable, 'processing/gold_layer.py'])

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE!")
    print("=" * 60)
    print("\nCheck outputs:")
    print("  - data/silver/stocks_clean.csv")
    print("  - data/gold/ohlc_5min.csv")
    print("  - data/gold/hourly_stats.csv")
    print("  - data/gold/symbol_summary.csv")


if __name__ == '__main__':
    run_processing()
