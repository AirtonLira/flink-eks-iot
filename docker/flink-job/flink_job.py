from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def create_kafka_source():
    return f"""
        CREATE TABLE crypto_prices (
            data STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'crypto-prices',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-group',
            'format' = 'raw'
        )
    """

def process_and_save_to_parquet(data):
    try:
        # Parse JSON data
        json_data = json.loads(data)
        
        # Create DataFrame structure
        records = []
        for crypto, info in json_data.items():
            record = {
                'cryptocurrency': crypto,
                'price_usd': info['usd'],
                'timestamp': info['timestamp']
            }
            records.append(record)
        
        # Create PyArrow Table
        table = pa.Table.from_pylist(records)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'/tmp/crypto_prices_{timestamp}.parquet'
        
        # Write to Parquet file
        pq.write_table(table, filename)
        
        return filename
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create Kafka source table
    t_env.execute_sql(create_kafka_source())

    # Create result table
    result_table = t_env.sql_query("SELECT data FROM crypto_prices")

    # Convert table to datastream
    ds = t_env.to_data_stream(result_table)

    # Process each record and save to Parquet
    ds.map(lambda row: process_and_save_to_parquet(row))

    # Execute the job
    env.execute("Crypto Prices Processing")

if __name__ == '__main__':
    main()
