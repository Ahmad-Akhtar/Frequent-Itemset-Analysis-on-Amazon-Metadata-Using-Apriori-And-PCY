from collections import defaultdict, deque
from itertools import combinations
from kafka import KafkaConsumer
import json
import statistics
from pymongo import MongoClient

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'data3'

# Parameters
window_size = 1000  # Number of transactions in the sliding window
min_support = 0.02  # Minimum support threshold
z_score_threshold = 1.5  # Threshold for detecting anomalies using Z-score

# MongoDB configuration
mongo_uri = 'mongodb://localhost:27017/'
mongo_db_name = 'Frequent_Itemset'
mongo_collection_name = 'anomalies'

# Initialize MongoDB client and collection
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
anomalies_collection = db[mongo_collection_name]

# Initialize Kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Initialize data structures
frequent_itemsets = defaultdict(int)
item_counts = defaultdict(int)
transaction_count = 0
historical_item_counts = defaultdict(list)  # Store historical counts for anomaly detection

def analyze_temporal_patterns(transaction):
    global transaction_count, frequent_itemsets, item_counts
    transaction_count += 1

    also_buy_items = transaction.get('also_buy', [])
    also_view_items = transaction.get('also_view', [])
    all_items = also_buy_items + also_view_items

    # Update frequent itemsets for pairs of items
    for pair in combinations(all_items, 2):
        frequent_itemsets[frozenset(pair)] += 1

    # Update item counts for anomaly detection
    for item in all_items:
        item_counts[item] += 1
        historical_item_counts[item].append(item_counts[item])

    # Detect anomalies based on Z-score
    if transaction_count > window_size:
        for item, counts in historical_item_counts.items():
            if len(counts) > 1:
                mean_count = statistics.mean(counts)
                std_dev = statistics.stdev(counts)

                # Calculate Z-score
                z_score = (item_counts[item] - mean_count) / std_dev

                # Check for anomaly
                if abs(z_score) > z_score_threshold:
                    anomaly_data = {
                        "item": item,
                        "z_score": z_score,
                        "transaction_count": transaction_count
                    }
                    print(f"Anomaly Detected: Item {item} has a Z-score of {z_score:.2f} at transaction {transaction_count}")
                    # Insert anomaly data into MongoDB
                    anomalies_collection.insert_one(anomaly_data)


# Main loop to process streaming data
sliding_window = deque(maxlen=window_size)

for message in consumer:
    transaction = message.value
    sliding_window.append(transaction)
    
    for txn in sliding_window:
        analyze_temporal_patterns(txn)

# Process remaining transactions in the sliding window
for txn in sliding_window:
    analyze_temporal_patterns(txn)

