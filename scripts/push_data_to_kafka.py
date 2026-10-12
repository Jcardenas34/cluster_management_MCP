#!/usr/bin/env python
"""
Sends a data stream to the kafka producer 
to be processed by the spark cluster and 
evaluated by the fraud detection server server.    
"""
import time
import json
import argparse
import sys
from typing import Union
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from fraud_detection.helpers import load_data_subset

def build_data_envelope(data, count:int) -> dict:
    '''
    Create the data envelope that will be sent to the 
    kafka producer.
    '''

    # Simulate credit card data
    data_to_transmit = {
        "id": count,
        "timestamp": time.time(),
        "msg": "card_transatction_{}".format(count)
    }

    # Fill envelope with transaction data from dataset
    for item in range(len(data)):
        data_to_transmit["feature_{}".format(item+1)] = data[item]
                
    return data_to_transmit


def initialize_broker(ip_addr:str) -> Union[KafkaProducer,None]:
    '''
    Attempts to initialize a Kafka broker
    located at given 'ip_addr'.
    '''
    try:
        producer = KafkaProducer(
        bootstrap_servers=[ip_addr],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    
    except NoBrokersAvailable:
        print(f"No kafka server present on {ip_addr}... is IP_ADDRESS correct?")
        sys.exit(1)

    

def main(args):
    '''
    Sends either training or synthetically create credit card transaction
    data to a kafka broker on the network.

    '''
    
    print(f"Starting producer. Sending data to {args.broker_ip}...")
    producer = initialize_broker(args.broker_ip)   


    # Define the dataset do you want to send, and load it (~2500 events)
    data_path = './dataset/credit_card_fraud.h5'
    if args.synthetic:
        print(f"Sending synthetic data to {args.broker_ip}...")
        data_path = "./dataset/synthetic_dataset.h5"

    data_points = load_data_subset(data_path=data_path, n_samples=-1)


    try:
        # Taking a note of how many instances have a z score larger than 2
        count = 0
        
        # Loop over dataset to send for throughput testing
        for _ in range(30):
            for data in data_points:
                
                transaction_data = build_data_envelope(data, count)

                # Transmit the data
                try:
                    producer.send('heartbeat-topic', value=transaction_data)
                    count += 1
                
                    if count % 100 == 0:
                        print(f"Sent {count} transactions...")
                        
                        # time.sleep(0.10) # Send 500 messages per second, the limit of 3 pis on a spark cluster
                        time.sleep(0.20) # Optimal, read and processing speed are comparable

                except TimeoutError:
                    print("Kafka Broker not present.")

                    
   
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", dest="synthetic", action="store_true", default=False)
    parser.add_argument("-ip", dest="broker_ip", default="192.168.1.61:9092") # This is Node 3 on pi cluster
    args = parser.parse_args()

    main(args)
