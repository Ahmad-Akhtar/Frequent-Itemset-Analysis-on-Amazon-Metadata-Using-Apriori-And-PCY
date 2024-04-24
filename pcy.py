from collections import defaultdict
from itertools import combinations
from kafka import KafkaConsumer
import json
from collections import deque
from pymongo import MongoClient

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'data3'

# Parameters
window_size = 1000  # Number of transactions in the sliding window
min_support = 0.02  # Minimum support threshold
mongo_uri = 'mongodb://localhost:27017/'
mongo_db_name = 'Frequent_Itemset'
mongo_frequent_collection_name = 'pcy_frequent_itemsets'
mongo_association_collection_name = 'pcy_association_rules'

# Initialize MongoDB client and collections
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
frequent_collection = db[mongo_frequent_collection_name]
association_collection = db[mongo_association_collection_name]

# Initialize data structures
frequent_itemsets = defaultdict(int)
pair_counts = defaultdict(int)
transaction_count = 0

# Initialize Kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def hash_function(item1, item2, table_size):
    return (hash(item1 + item2) % table_size)

def update_pair_counts(transaction, table_size):
    global transaction_count, pair_counts
    transaction_count += 1

    # Extract 'also_buy' and 'also_view' values from the transaction
    also_buy_items = transaction.get('also_buy', [])
    also_view_items = transaction.get('also_view', [])
    all_items = also_buy_items + also_view_items

    # Update pair counts
    for pair in combinations(all_items, 2):
        hash_value = hash_function(pair[0], pair[1], table_size)
        pair_counts[pair] += 1
        print(f"Pair: {pair}, Count: {pair_counts[pair]}, Support: {pair_counts[pair] / transaction_count:.4f} in {transaction_count} transactions")

def update_frequent_pairs_with_pcy():
    global transaction_count, frequent_itemsets, pair_counts
    for pair, count in pair_counts.items():
        if count >= min_support * transaction_count:
            frequent_itemsets[pair] += count

            # Insert into MongoDB
            frequent_collection.insert_one({
                "itemset": list(pair),
                "count": count,
                "support": count / transaction_count,
                "transactions": transaction_count
            })


def generate_association_rules(frequent_itemsets, min_confidence):
    generated_rules = 0  # Counter for generated rules
    for itemset, count in frequent_itemsets.items():
        if len(itemset) > 1:  # Only consider itemsets with more than one item
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = frozenset(itemset) - antecedent
                    
                    if antecedent in frequent_itemsets and consequent in frequent_itemsets:
                        support_antecedent = frequent_itemsets[antecedent] / transaction_count
                        support_consequent = frequent_itemsets[consequent] / transaction_count
                        confidence = support_antecedent / support_consequent
                        
                        if confidence >= min_confidence:
                            generated_rules += 1  # Increment the counter
                            # Insert into MongoDB
                            association_collection.insert_one({
                                "antecedent": list(antecedent),
                                "consequent": list(consequent),
                                "confidence": confidence,
                                "support_antecedent": support_antecedent,
                                "support_consequent": support_consequent,
                                "transactions": transaction_count
                            })
                            print(f"Association Rule: {antecedent} -> {consequent}, Confidence: {confidence:.4f}")
    
    print(f"Generated {generated_rules} association rules.")





        
# Generate association rules
min_confidence = 0.6  # Set a minimum confidence threshold

# Maximum number of transactions to process
max_transactions = 10  # Set this to your desired number of transactions

# Main loop to process streaming data
sliding_window = deque(maxlen=window_size)
transaction_counter = 0  # Counter for processed transactions

print("Starting consumer...")
for message in consumer:
    if transaction_counter >= max_transactions:
        print(f"Processed {max_transactions} transactions. Stopping...")
        break
    
    transaction = message.value
    
    # Add transaction to sliding window
    sliding_window.append(transaction)
    
    # Update pair counts based on transactions in sliding window
    for txn in sliding_window:
        update_pair_counts(txn, len(sliding_window) * window_size)  # Use a large table size for simplicity
        
    # Update frequent pairs using PCY algorithm
    update_frequent_pairs_with_pcy()
    
    transaction_counter += 1

generate_association_rules(frequent_itemsets, min_confidence)




