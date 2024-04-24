from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
import mmh3  # MurmurHash3 for hashing

class PCYAlgorithm:
    def __init__(self, topic, bootstrap_servers, window_size, hash_table_size, min_support, min_confidence):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.window_size = window_size
        self.hash_table_size = hash_table_size
        self.min_support = min_support
        self.min_confidence = min_confidence
        
        # Create Kafka consumer
        self.consumer = KafkaConsumer(topic,
                                      group_id='my_consumer_group',
                                      bootstrap_servers=bootstrap_servers,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
        # Initialize sliding window
        self.window = []
        
        # Initialize hash table
        self.hash_table = [0] * hash_table_size
        
        # Dictionary to store itemsets and their counts within the sliding window
        self.itemsets_count = defaultdict(int)
        
        # Total number of transactions
        self.total_transactions = 0
        
        # Frequent itemsets
        self.frequent_itemsets = []
        
        # Association rules
        self.association_rules = []
    
    def hash_function(self, itemset):
        # Use MurmurHash3 to hash itemsets
        return mmh3.hash(str(itemset)) % self.hash_table_size
    
    def process_transaction(self, transaction):
        # Remove oldest transaction from the window if it exceeds the maximum window size
        if len(self.window) == self.window_size:
            old_transaction = self.window.pop(0)
            for pair in combinations(old_transaction, 2):
                hash_value = self.hash_function(pair)
                self.hash_table[hash_value] -= 1
        
        # Add new transaction to the window
        self.window.append(transaction)
        
        # Update itemsets count based on transactions in the window
        for item in transaction:
            self.itemsets_count[item] += 1
        
        # Increment total transactions count
        self.total_transactions = len(self.window)
        
        # Count item pairs using hashing
        for pair in combinations(transaction, 2):
            hash_value = self.hash_function(pair)
            self.hash_table[hash_value] += 1
    
    def find_frequent_itemsets(self):
    # Calculate support for each itemset
        self.frequent_itemsets = [(itemset, count / self.total_transactions) for itemset, count in self.itemsets_count.items()
                              if count / self.total_transactions >= self.min_support]

    def generate_association_rules(self):
        # Generate association rules
        self.association_rules = []
        for itemset, _ in self.frequent_itemsets:
            if len(itemset) > 1:
                for i in range(1, len(itemset)):
                    for subset in combinations(itemset, i):
                        antecedent = subset
                        consequent = tuple(item for item in itemset if item not in subset)
                        antecedent_support = next((support for item, support in self.frequent_itemsets if item == antecedent), None)
                        consequent_support = next((support for item, support in self.frequent_itemsets if item == consequent), None)
                        if antecedent_support is not None and consequent_support is not None:
                            confidence = antecedent_support / consequent_support
                            if confidence >= self.min_confidence:
                                self.association_rules.append((antecedent, consequent, confidence))
    
    def print_real_time_insights(self):
        print('Real-time insights:')
        print('Total transactions within the sliding window:', self.total_transactions)
        print('-' * 50)
        
        print('Frequent itemsets within the sliding window:')
        for itemset, support in self.frequent_itemsets:
            print(itemset, ':', support)
        
        print('-' * 50)
        
        print('Association rules:')
        for antecedent, consequent, confidence in self.association_rules:
            print(antecedent, '->', consequent, ':', confidence)
        
        print('-' * 50)
    
    def run(self):
        # Process streaming data within the sliding window
        for message in self.consumer:
            data = message.value
            description = data['description'][0]
            items = description.split()
            self.process_transaction(items)
            self.find_frequent_itemsets()
            self.generate_association_rules()
            self.print_real_time_insights()
        
        # Close Kafka consumer
        self.consumer.close()

# Configuration parameters
topic = 'preprocessed_data'
bootstrap_servers = ['localhost:9092']
window_size = 100
hash_table_size = 1000
min_support = 0.1
min_confidence = 0.5

# Create and run PCY algorithm instance
pcy_algorithm = PCYAlgorithm(topic, bootstrap_servers, window_size, hash_table_size, min_support, min_confidence)
pcy_algorithm.run()

