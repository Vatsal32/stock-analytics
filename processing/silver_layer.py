import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverLayer:
    def __init__(self):
        self.bronze_path = Path('./data/bronze')
        self.silver_path = Path('./data/silver')
        self.silver_path.mkdir(parents=True, exist_ok=True)

    def process(self):
        """Process bronze data into silver layer"""
        logger.info("Starting silver layer processing...")

        # Read all bronze files
        bronze_files = list(self.bronze_path.glob('stocks_*.csv'))

        if not bronze_files:
            logger.warning("No bronze files found")
            return

        dfs = []
        for file in bronze_files:
            df = pd.read_csv(file)
            dfs.append(df)

        # Combine all data
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(df)} records from bronze layer")

        # Data quality checks and transformations
        # 1. Remove duplicates
        original_count = len(df)
        df = df.drop_duplicates(subset=['timestamp', 'symbol'])
        logger.info(f"Removed {original_count - len(df)} duplicates")

        # 2. Handle missing values
        df = df.dropna(subset=['price', 'symbol'])

        # 3. Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # 4. Add derived columns
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute

        # 5. Sort by timestamp
        df = df.sort_values('timestamp')

        # Write to silver layer
        output_file = self.silver_path / 'stocks_clean.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Written {len(df)} clean records to {output_file}")

        return df


if __name__ == '__main__':
    silver = SilverLayer()
    silver.process()
