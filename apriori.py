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
min_support = 0.4  # Minimum support threshold
min_confidence = 2.0  # Minimum confidence threshold

# MongoDB configuration
mongo_uri = 'mongodb://localhost:27017/'
mongo_db_name = 'Frequent_Itemset'
mongo_frequent_collection_name = 'apriori'
mongo_association_collection_name = 'apriori_associationrules'

# Initialize MongoDB client and collections
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
frequent_collection = db[mongo_frequent_collection_name]
association_collection = db[mongo_association_collection_name]

# Initialize data structures
frequent_itemsets = defaultdict(int)
item_counts = defaultdict(int)  # Counter for each item
transaction_count = 0

# Initialize Kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def update_frequent_single_items(transaction):
    global transaction_count, item_counts
    transaction_count += 1

    # Extract 'also_buy' and 'also_view' values from the transaction
    also_buy_items = transaction.get('also_buy', [])
    also_view_items = transaction.get('also_view', [])
    all_items = also_buy_items + also_view_items

    # Update frequent itemsets and item counts for single items
    for item in all_items:
        item_counts[item] += 1

    # Print frequent single items with correct support and insert into MongoDB
    for item, count in item_counts.items():
        calculated_support = count / transaction_count
        if calculated_support >= min_support:
            print(f"Item: {item}, Count: {count}, Support: {calculated_support:.4f} in {transaction_count} transactions")
            # Insert into MongoDB
            frequent_collection.insert_one({
                "item": item,
                "count": count,
                "support": calculated_support,
                "transactions": transaction_count
            })

def update_frequent_pairs(transaction):
    global transaction_count, frequent_itemsets
    transaction_count += 1

    # Extract 'also_buy' and 'also_view' values from the transaction
    also_buy_items = transaction.get('also_buy', [])
    also_view_items = transaction.get('also_view', [])
    all_items = also_buy_items + also_view_items

    # Update frequent itemsets for pairs of items
    for pair in combinations(all_items, 2):
        frequent_itemsets[frozenset(pair)] += 1

    # Print frequent pairs with correct support and insert into MongoDB
    for itemset, count in frequent_itemsets.items():
        if len(itemset) == 2:  # Check if it's a pair
            calculated_support = count / transaction_count
            if calculated_support >= min_support:
                item1, item2 = itemset
                print(f"Pair: ({item1}, {item2}), Count: {count}, Support: {calculated_support:.4f} in {transaction_count} transactions")
                # Insert into MongoDB
                frequent_collection.insert_one({
                    "itemset": list(itemset),
                    "count": count,
                    "support": calculated_support,
                    "transactions": transaction_count
                })

def update_frequent_triplets(transaction):
    global transaction_count, frequent_itemsets
    transaction_count += 1

    # Extract 'also_buy' and 'also_view' values from the transaction
    also_buy_items = transaction.get('also_buy', [])
    also_view_items = transaction.get('also_view', [])
    all_items = also_buy_items + also_view_items

    # Update frequent itemsets for triplets of items
    for triplet in combinations(all_items, 3):
        frequent_itemsets[frozenset(triplet)] += 1

    # Print frequent triplets with correct support and insert into MongoDB
    for itemset, count in frequent_itemsets.items():
        if len(itemset) == 3:  # Check if it's a triplet
            calculated_support = count / transaction_count
            if calculated_support >= min_support:
                print(f"Triplet: {itemset}, Count: {count}, Support: {calculated_support:.4f} in {transaction_count} transactions")
                # Insert into MongoDB
                frequent_collection.insert_one({
                    "itemset": list(itemset),
                    "count": count,
                    "support": calculated_support,
                    "transactions": transaction_count
                })

def generate_association_rules(frequent_itemsets):
    for itemset, count in frequent_itemsets.items():
        if len(itemset) > 1:  # Only consider itemsets with more than one item
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = itemset - antecedent

                    if antecedent in frequent_itemsets and consequent in frequent_itemsets:
                        support_antecedent = frequent_itemsets[antecedent] / transaction_count
                        support_consequent = frequent_itemsets[consequent] / transaction_count
                        confidence = support_antecedent / support_consequent

                        if confidence >= min_confidence:
                            print(f"Association Rule: {antecedent} -> {consequent}, Confidence: {confidence:.4f}")
                            # Insert into MongoDB
                            association_collection.insert_one({
                                "antecedent": list(antecedent),
                                "consequent": list(consequent),
                                "confidence": confidence,
                                "support_antecedent": support_antecedent,
                                "support_consequent": support_consequent,
                                "transactions": transaction_count
                            })

# Main loop to process streaming data
for message in consumer:
    transaction = message.value  # Assuming message.value is a dictionary
    # Update frequent single items, pairs, and triplets based on the transaction
    update_frequent_single_items(transaction)
    update_frequent_pairs(transaction)
    update_frequent_triplets(transaction)
    # Generate association rules
    generate_association_rules(frequent_itemsets)
