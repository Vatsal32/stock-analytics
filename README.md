# Real-Time Stock Analytics Pipeline

## Architecture

```
Stock API → Producer → Kafka → Consumer → Bronze Layer (CSV)
                                              ↓
                                         Silver Layer (Cleaned)
                                              ↓
                                         Gold Layer (Aggregations)
```

## Tech Stack

- **Python 3.9+**: Core language
- **Apache Kafka**: Message streaming
- **Pandas**: Data processing
- **Docker**: Container orchestration

## Features

✅ Real-time stock price ingestion from Alpha Vantage API\
✅ Event-driven architecture with Kafka\
✅ Medallion architecture (Bronze → Silver → Gold)\
✅ Consumer groups for parallel processing\
✅ Offset management for fault tolerance\
✅ Time-series aggregations (5-min OHLC, hourly stats)

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Kafka
```bash
docker-compose up -d
```

### 3. Configure API Key
Create `.env` file:
```
ALPHA_VANTAGE_API_KEY=your_key_here
```

### 4. Run Pipeline

Terminal 1 - Producer:
```bash
python producers/stock_producer.py
```

Terminal 2 - Consumer:
```bash
python consumers/stock_consumer.py
```

Terminal 3 - Processing (after data collected):
```bash
python processing/silver_layer.py
python processing/gold_layer.py
```

## Data Flow

1. **Bronze Layer**: Raw stock data from Kafka
2. **Silver Layer**: Cleaned, deduplicated, with derived fields
3. **Gold Layer**: Business metrics and aggregations

## Performance Metrics

- Throughput: 1000+ messages/minute
- Latency: <100ms producer-to-consumer
- Processing: ~500 records/second in aggregations

## Key Learnings

- Kafka consumer groups enable horizontal scaling
- Offset management provides fault tolerance
- Medallion architecture separates concerns
- Batch processing improves write efficiency