"""
Data generation utilities for creating sample datasets.

This module generates realistic sample data for learning PySpark.
It creates various datasets that will be used across different jobs.
"""

import random
import json
import csv
from datetime import datetime, timedelta
from faker import Faker
import os


fake = Faker()
Faker.seed(42)  # For reproducible data
random.seed(42)


def generate_users(num_users=1000):
    """
    Generate sample user data.
    
    Returns list of user dictionaries with:
    - user_id, name, email, age, city, country, signup_date
    """
    users = []
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia']
    
    for i in range(1, num_users + 1):
        signup_date = fake.date_between(start_date='-2y', end_date='today')
        user = {
            'user_id': i,
            'name': fake.name(),
            'email': fake.email(),
            'age': random.randint(18, 75),
            'city': fake.city(),
            'country': random.choice(countries),
            'signup_date': signup_date.strftime('%Y-%m-%d')
        }
        users.append(user)
    
    return users


def generate_products(num_products=200):
    """
    Generate sample product data.
    
    Returns list of product dictionaries with:
    - product_id, name, category, price, brand, rating
    """
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    
    products = []
    for i in range(1, num_products + 1):
        product = {
            'product_id': i,
            'name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(9.99, 999.99), 2),
            'brand': random.choice(brands),
            'rating': round(random.uniform(1.0, 5.0), 1),
            'stock_quantity': random.randint(0, 500)
        }
        products.append(product)
    
    return products


def generate_transactions(num_transactions=5000, num_users=1000, num_products=200):
    """
    Generate sample transaction/order data.
    
    Returns list of transaction dictionaries with:
    - transaction_id, user_id, product_id, quantity, amount, transaction_date
    """
    transactions = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1, num_transactions + 1):
        transaction_date = start_date + timedelta(days=random.randint(0, 365))
        quantity = random.randint(1, 5)
        
        transaction = {
            'transaction_id': i,
            'user_id': random.randint(1, num_users),
            'product_id': random.randint(1, num_products),
            'quantity': quantity,
            'amount': round(random.uniform(10.0, 500.0), 2),
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'status': random.choice(['completed', 'completed', 'completed', 'pending', 'cancelled'])
        }
        transactions.append(transaction)
    
    return transactions


def generate_product_reviews(num_reviews=3000, num_users=1000, num_products=200):
    """
    Generate sample product review data for text processing/search.
    
    Returns list of review dictionaries with:
    - review_id, user_id, product_id, rating, review_text, review_date
    """
    reviews = []
    
    review_texts = [
        "Great product! Highly recommend it.",
        "Not worth the price. Quality could be better.",
        "Amazing! Exceeded my expectations.",
        "Decent product for the price.",
        "Terrible experience. Would not buy again.",
        "Love it! Best purchase I've made.",
        "Average quality, nothing special.",
        "Fantastic! Will buy again.",
        "Disappointed with the quality.",
        "Perfect! Exactly what I needed."
    ]
    
    for i in range(1, num_reviews + 1):
        review_date = fake.date_between(start_date='-1y', end_date='today')
        rating = random.randint(1, 5)
        
        review = {
            'review_id': i,
            'user_id': random.randint(1, num_users),
            'product_id': random.randint(1, num_products),
            'rating': rating,
            'review_text': random.choice(review_texts) + " " + fake.sentence(),
            'review_date': review_date.strftime('%Y-%m-%d')
        }
        reviews.append(review)
    
    return reviews


def generate_clickstream(num_events=10000, num_users=1000, num_products=200):
    """
    Generate sample clickstream/event data.
    
    Returns list of event dictionaries with:
    - event_id, user_id, product_id, event_type, timestamp
    """
    events = []
    event_types = ['view', 'view', 'view', 'click', 'click', 'add_to_cart', 'purchase']
    
    start_time = datetime.now() - timedelta(days=7)
    
    for i in range(1, num_events + 1):
        event_time = start_time + timedelta(seconds=random.randint(0, 7*24*60*60))
        
        event = {
            'event_id': i,
            'user_id': random.randint(1, num_users),
            'product_id': random.randint(1, num_products),
            'event_type': random.choice(event_types),
            'timestamp': event_time.strftime('%Y-%m-%d %H:%M:%S'),
            'session_id': random.randint(1, 5000)
        }
        events.append(event)
    
    return events


def save_to_csv(data, filename, data_dir):
    """Save data to CSV file."""
    filepath = os.path.join(data_dir, filename)
    
    if data:
        keys = data[0].keys()
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f"✓ Generated {filepath} ({len(data)} records)")


def save_to_json(data, filename, data_dir):
    """Save data to JSON file."""
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✓ Generated {filepath} ({len(data)} records)")


def generate_all_datasets(data_dir):
    """
    Generate all sample datasets and save them to the data directory.
    
    Args:
        data_dir (str): Directory to save generated data files
    """
    print("\n📊 Generating sample datasets...")
    print("=" * 60)
    
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate datasets
    users = generate_users(1000)
    products = generate_products(200)
    transactions = generate_transactions(5000, 1000, 200)
    reviews = generate_product_reviews(3000, 1000, 200)
    clickstream = generate_clickstream(10000, 1000, 200)
    
    # Save as CSV
    save_to_csv(users, 'users.csv', data_dir)
    save_to_csv(products, 'products.csv', data_dir)
    save_to_csv(transactions, 'transactions.csv', data_dir)
    save_to_csv(reviews, 'reviews.csv', data_dir)
    save_to_csv(clickstream, 'clickstream.csv', data_dir)
    
    # Also save some as JSON for variety
    save_to_json(users[:100], 'users_sample.json', data_dir)
    save_to_json(products, 'products.json', data_dir)
    
    print("=" * 60)
    print("✓ All datasets generated successfully!\n")


if __name__ == "__main__":
    # When run directly, generate all datasets
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data")
    generate_all_datasets(data_dir)
