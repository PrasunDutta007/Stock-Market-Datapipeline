# Stock Market Data Pipeline

A real-time and batch data pipeline for stock market data using 
AlphaVantage, Kafka, Spark, MinIO, Airflow, and Snowflake.

## Architecture
- **Batch**: AlphaVantage → Kafka → MinIO → Spark → Snowflake (Airflow orchestrated)
- **Streaming**: AlphaVantage → Kafka → MinIO → Spark

## Setup
1. Copy `.env.example` to `.env` and fill in your credentials
2. Run `docker-compose up -d`
3. Trigger the DAG in Airflow at http://localhost:8081

## Requirements
- Docker + Docker Compose
- AlphaVantage API key (free tier works)
- Snowflake account