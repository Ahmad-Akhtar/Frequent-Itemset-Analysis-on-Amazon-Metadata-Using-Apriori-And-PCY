from collections import defaultdict
from itertools import combinations
from kafka import KafkaConsumer
import json
from collections import deque

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'data3'

# Parameters
window_size = 1000  # Number of transactions in the sliding window
min_support = 0.02  # Minimum support threshold

# Initialize data structures
frequent_itemsets = defaultdict(int)
item_counts = defaultdict(int)
pair_counts = defaultdict(int)  # Counter for pairs of items
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
        pair_counts[hash_value] += 1

def update_frequent_pairs_with_pcy():
    global transaction_count, frequent_itemsets, pair_counts
    for hash_value, count in pair_counts.items():
        if count >= min_support * transaction_count:
            pair = hash_value  # For simplicity, assuming hash value itself is the pair
            frequent_itemsets[pair] += count

            # Print frequent pair with correct support
            calculated_support = count / transaction_count
            if calculated_support >= min_support:
                print(f"Pair: {pair}, Count: {count}, Support: {calculated_support:.4f} in {transaction_count} transactions")



def generate_association_rules(frequent_itemsets, min_confidence):
    print("Association Rules:")
    for itemset, count in frequent_itemsets.items():
        if isinstance(itemset, int):
            continue

        if len(itemset) > 1:  # Only consider itemsets with more than one item
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = frozenset(antecedent)
                    consequent = itemset - antecedent
                    
                    if antecedent in frequent_itemsets and consequent in frequent_itemsets:
                        support_antecedent = frequent_itemsets[antecedent] / transaction_count
                        support_consequent = frequent_itemsets[consequent] / transaction_count
                        confidence = support_antecedent / support_consequent

                        print(f"Antecedent: {antecedent}, Consequent: {consequent}, Support Antecedent: {support_antecedent:.4f}, Support Consequent: {support_consequent:.4f}, Confidence: {confidence:.4f}") 

                        if confidence >= min_confidence:
                            print(f"Association Rule: {antecedent} -> {consequent}, Confidence: {confidence:.4f}")  # Print confidence
        else:
            print(f"Itemset length is not greater than 1: {itemset}")




# Main loop to process streaming data
sliding_window = deque(maxlen=window_size)

# Counter for processed transactions
processed_transactions = 0
# Maximum number of transactions to process
max_transactions = 20  # Set this to your desired number of transactions

for message in consumer:
    transaction = message.value
    
    # Add transaction to sliding window
    sliding_window.append(transaction)
    
    # Update pair counts based on transactions in sliding window
    for txn in sliding_window:
        update_pair_counts(txn, len(sliding_window) * window_size)  # Use a large table size for simplicity
        
    # Update frequent pairs using PCY algorithm
    update_frequent_pairs_with_pcy()
    
    # Increment processed transactions counter
    processed_transactions += 1
    
    # Check if reached the desired number of transactions
    if processed_transactions >= max_transactions:
        print(f"Processed {max_transactions} transactions. Stopping...")
        break

# Generate association rules
min_confidence = 0.01  # Set a minimum confidence threshold
generate_association_rules(frequent_itemsets, min_confidence)

