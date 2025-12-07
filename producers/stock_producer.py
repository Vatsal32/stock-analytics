import os
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def generate_mock_data(symbol):
    """Generate mock data when API limit reached"""
    import random
    base_prices = {'AAPL': 185, 'GOOGL': 142, 'MSFT': 380, 'AMZN': 155, 'TSLA': 242}
    base = base_prices.get(symbol, 100)

    return {
        'symbol': symbol,
        'price': round(base + random.uniform(-2, 2), 2),
        'volume': random.randint(1000000, 5000000),
        'timestamp': datetime.utcnow().isoformat(),
        'change': round(random.uniform(-1, 1), 2),
        'change_percent': f"{round(random.uniform(-0.5, 0.5), 2)}%"
    }


class StockProducer:
    def __init__(self):
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-prices')

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3
        )

        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

    def fetch_stock_price(self, symbol):
        """Fetch real-time stock price from Alpha Vantage"""
        try:
            url = f'https://www.alphavantage.co/query'
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.api_key
            }

            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if 'Global Quote' in data and data['Global Quote']:
                quote = data['Global Quote']
                return {
                    'symbol': symbol,
                    'price': float(quote.get('05. price', 0)),
                    'volume': int(quote.get('06. volume', 0)),
                    'timestamp': datetime.utcnow().isoformat(),
                    'change': float(quote.get('09. change', 0)),
                    'change_percent': quote.get('10. change percent', '0%')
                }
            else:
                logger.warning(f"No data for {symbol}, using mock data")
                # Fallback to mock data if API limit reached
                return generate_mock_data(symbol)

        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return generate_mock_data(symbol)

    def produce(self):
        """Main producer loop"""
        logger.info(f"Starting producer for symbols: {self.symbols}")
        message_count = 0

        try:
            while True:
                for symbol in self.symbols:
                    stock_data = self.fetch_stock_price(symbol)

                    # Send to Kafka
                    self.producer.send(
                        self.topic,
                        value=stock_data,
                        key=symbol.encode('utf-8')
                    )

                    message_count += 1
                    logger.info(f"[{message_count}] Published {symbol}: ${stock_data['price']} "
                                f"({stock_data['change_percent']})")

                # Flush to ensure delivery
                self.producer.flush()

                # Wait before next fetch (respect API limits)
                time.sleep(60)  # Fetch every minute

        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            self.producer.close()


if __name__ == '__main__':
    producer = StockProducer()
    producer.produce()
