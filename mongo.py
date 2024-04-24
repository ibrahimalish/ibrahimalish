from kafka import KafkaConsumer
import json
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce']
product_collection = db['preprocessed_data']
interaction_collection = db['user_interactions']
recommendation_collection = db['recommendations']

# Define Kafka consumer
consumer = KafkaConsumer('preprocessed_data',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Load preprocessed product data
product_data = list(product_collection.find())

# Initialize user-item interaction matrix
user_item_matrix = {}

# Function to update user-item interaction matrix
def update_user_item_matrix(user_id, product_id):
    if user_id not in user_item_matrix:
        user_item_matrix[user_id] = {}
    user_item_matrix[user_id][product_id] = user_item_matrix[user_id].get(product_id, 0) + 1

# Function to recommend items to a user based on hybrid filtering
def recommend_items(user_id, top_n=5):
    if user_id not in user_item_matrix:
        return "No interactions found for this user."
    
    user_interactions = user_item_matrix[user_id]
    user_vector = np.array([user_interactions.get(product_id, 0) for product_id in product_dict])
    
    # Content-based filtering
    tfidf_vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = tfidf_vectorizer.fit_transform([product['title'] for product in product_data])
    cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_vectorizer.transform([product['title'] for product in product_data]))
    content_based_scores = np.dot(user_vector, cosine_similarities)
    
    # Collaborative filtering
    similarity_scores = []
    for other_user_id, other_user_interactions in user_item_matrix.items():
        if other_user_id != user_id:
            other_user_vector = np.array([other_user_interactions.get(product_id, 0) for product_id in product_dict])
            similarity = np.dot(user_vector, other_user_vector) / (np.linalg.norm(user_vector) * np.linalg.norm(other_user_vector))
            similarity_scores.append((other_user_id, similarity))
    
    similarity_scores.sort(key=lambda x: x[1], reverse=True)
    collaborative_scores = np.zeros(len(product_dict))
    for _, similarity in similarity_scores:
        for product_id, interaction_count in user_item_matrix[_].items():
            collaborative_scores[product_id] += interaction_count * similarity
    
    # Hybrid recommendation
    hybrid_scores = content_based_scores + collaborative_scores
    recommended_indices = np.argsort(hybrid_scores)[::-1][:top_n]
    recommended_titles = [product_data[i]['title'] for i in recommended_indices]
    
    return recommended_titles

# Consume user interaction data from Kafka topic and update user-item matrix
for message in consumer:
    interaction_data = message.value
    user_id = interaction_data['user_id']
    product_id = interaction_data['asin']
    update_user_item_matrix(user_id, product_id)
    
    # Get recommendations for the user
    recommendations = recommend_items(user_id)
    
    # Store recommendations in MongoDB
    recommendation_collection.insert_one({'user_id': user_id, 'recommendations': recommendations})

# Close the Kafka consumer
consumer.close()

