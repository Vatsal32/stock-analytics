import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoldLayer:
    def __init__(self):
        self.silver_path = Path('./data/silver')
        self.gold_path = Path('./data/gold')
        self.gold_path.mkdir(parents=True, exist_ok=True)

    def process(self):
        """Create gold layer aggregations"""
        logger.info("Starting gold layer processing...")

        # Read silver data
        silver_file = self.silver_path / 'stocks_clean.csv'

        if not silver_file.exists():
            logger.error("Silver layer file not found. Run silver_layer.py first")
            return

        df = pd.read_csv(silver_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        logger.info(f"Loaded {len(df)} records from silver layer")

        # Aggregation 1: 5-minute OHLC (Open, High, Low, Close)
        df_5min = df.set_index('timestamp').groupby([
            pd.Grouper(freq='5Min'),
            'symbol'
        ]).agg({
            'price': ['first', 'max', 'min', 'last', 'mean'],
            'volume': 'sum'
        }).reset_index()

        df_5min.columns = ['window_start', 'symbol', 'open', 'high', 'low', 'close', 'avg_price', 'total_volume']
        df_5min['window_end'] = df_5min['window_start'] + pd.Timedelta(minutes=5)

        output_5min = self.gold_path / 'ohlc_5min.csv'
        df_5min.to_csv(output_5min, index=False)
        logger.info(f"Created 5-minute OHLC aggregations: {output_5min}")

        # Aggregation 2: Hourly statistics
        df_hourly = df.set_index('timestamp').groupby([
            pd.Grouper(freq='1h'),
            'symbol'
        ]).agg({
            'price': ['mean', 'std', 'min', 'max'],
            'volume': 'sum',
            'change': 'sum'
        }).reset_index()

        df_hourly.columns = ['hour', 'symbol', 'avg_price', 'price_volatility',
                             'min_price', 'max_price', 'total_volume', 'total_change']

        output_hourly = self.gold_path / 'hourly_stats.csv'
        df_hourly.to_csv(output_hourly, index=False)
        logger.info(f"Created hourly statistics: {output_hourly}")

        # Aggregation 3: Symbol summary
        df_summary = df.groupby('symbol').agg({
            'price': ['mean', 'std', 'min', 'max', 'count'],
            'volume': 'sum',
            'change': 'sum'
        }).reset_index()

        df_summary.columns = ['symbol', 'avg_price', 'price_std', 'min_price',
                              'max_price', 'record_count', 'total_volume', 'total_change']

        output_summary = self.gold_path / 'symbol_summary.csv'
        df_summary.to_csv(output_summary, index=False)
        logger.info(f"Created symbol summary: {output_summary}")

        logger.info("Gold layer processing complete!")


if __name__ == '__main__':
    gold = GoldLayer()
    gold.process()
