import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,dogecoin",
        "vs_currencies": "usd",
        "include_last_updated_at": True
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Add timestamp
        for crypto in data:
            data[crypto]['timestamp'] = datetime.now().isoformat()
        
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def main():
    producer = create_kafka_producer()
    
    while True:
        try:
            crypto_data = get_crypto_prices()
            
            if crypto_data:
                producer.send('crypto-prices', value=crypto_data)
                print(f"Sent data to Kafka: {crypto_data}")
            
            # Wait for 1 minute before next request (CoinGecko API rate limit)
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
