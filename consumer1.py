# Import required libraries
from kafka import KafkaConsumer
import json

# Kafka consumer configuration
consumer = KafkaConsumer('preprocessed_data',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='apriori-consumer')

# Function to preprocess the data received from Kafka
def preprocess_data(data):
    # Extract relevant items from the data
    items = []
    if 'title' in data:
        items.append(data['title'])
    if 'asin' in data:
        items.append(data['asin'])
    # Add more item extraction logic as needed
    return set(items)

def apriori(transactions, min_support):
    # Step 1: Generate frequent 1-itemsets
    itemsets = {}
    for transaction in transactions:
        for item in transaction:
            if item in itemsets:
                itemsets[item] += 1
            else:
                itemsets[item] = 1
    
    # Step 2: Prune infrequent 1-itemsets
    num_transactions = len(transactions)
    frequent_itemsets = {item: support for item, support in itemsets.items() if support / num_transactions >= min_support}
    
    # Step 3: Generate frequent itemsets
    k = 2
    while True:
        # Generate candidate itemsets
        candidate_itemsets = {}
        for i in range(len(transactions)):
            for j in range(i + 1, len(transactions)):
                transaction_union = transactions[i] | transactions[j]
                for itemset in transaction_union:
                    if len(transaction_union) == k and itemset not in candidate_itemsets:
                        candidate_itemsets[itemset] = 1
                    elif len(transaction_union) == k and itemset in candidate_itemsets:
                        candidate_itemsets[itemset] += 1
        
        # Prune infrequent candidate itemsets
        frequent_itemsets.update({itemset: support for itemset, support in candidate_itemsets.items() if support / num_transactions >= min_support})
        
        # Break if no frequent itemsets found
        if not frequent_itemsets:
            break
        
        k += 1
    
    return frequent_itemsets
# Main function to consume data and perform frequent itemset mining
def consume_and_mine():
    transactions = []
    for message in consumer:
        # Decode and preprocess the received message
        data = json.loads(message.value.decode('utf-8'))
        transaction = preprocess_data(data)
        transactions.append(transaction)
        
        # Perform Apriori algorithm to find frequent itemsets
        frequent_itemsets, association_rules = apriori(transactions, min_support=0.1)
        
        # Print real-time insights and associations
        print("Frequent Itemsets:")
        for itemset in frequent_itemsets:
            print(itemset)
        
        print("Association Rules:")
        for rule in association_rules:
            print(rule)

# Run the consumer application
if __name__ == "__main__":
    consume_and_mine()

