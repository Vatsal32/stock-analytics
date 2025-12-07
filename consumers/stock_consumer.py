import os
import json
import csv
from datetime import datetime
from kafka import KafkaConsumer
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockConsumer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-prices')
        self.group_id = 'stock-processors'

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )

        # Create data directory
        self.bronze_path = Path('./data/bronze')
        self.bronze_path.mkdir(parents=True, exist_ok=True)

        self.batch = []
        self.batch_size = 10

    def write_batch(self):
        """Write batch to CSV file"""
        if not self.batch:
            return

        date_str = datetime.now().strftime('%Y%m%d')
        filename = self.bronze_path / f'stocks_{date_str}.csv'

        # Check if file exists to write header
        file_exists = filename.exists()

        with open(filename, 'a', newline='') as f:
            fieldnames = ['timestamp', 'symbol', 'price', 'volume', 'change', 'change_percent']
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            for record in self.batch:
                writer.writerow({
                    'timestamp': record['timestamp'],
                    'symbol': record['symbol'],
                    'price': record['price'],
                    'volume': record['volume'],
                    'change': record['change'],
                    'change_percent': record['change_percent']
                })

        logger.info(f"Written {len(self.batch)} records to {filename}")
        self.batch = []

    def consume(self):
        """Main consumer loop"""
        logger.info(f"Starting consumer, listening to topic: {self.topic}")
        record_count = 0

        try:
            for message in self.consumer:
                stock_data = message.value
                self.batch.append(stock_data)
                record_count += 1

                logger.info(f"[{record_count}] Consumed {stock_data['symbol']}: ${stock_data['price']}")

                # Write batch when size reached
                if len(self.batch) >= self.batch_size:
                    self.write_batch()

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
            self.write_batch()  # Write remaining records
        finally:
            self.consumer.close()


if __name__ == '__main__':
    consumer = StockConsumer()
    consumer.consume()