"""
Job 5: Search Indexing and Text Processing

Learning Objectives:
- Text processing and tokenization
- Building inverted indexes for search
- TF-IDF scoring for relevance
- Product recommendations
- Text similarity and matching

Key Concepts:
- Inverted indexes map terms to documents
- TF-IDF measures term importance
- Text processing is essential for search and NLP
- Spark handles large-scale text processing efficiently
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, explode, split, lower, trim, regexp_replace, collect_list, size,
    count, sum as spark_sum, desc, lit, array_contains, when,
    concat_ws, collect_set, udf, length, row_number, dense_rank
)
from pyspark.sql.types import ArrayType, StringType, FloatType
from utils.spark_session import get_spark_session, stop_spark_session, get_data_dir
from utils.data_generator import generate_all_datasets


def text_preprocessing(reviews_df):
    """
    LESSON 1: Text Preprocessing
    
    Clean and normalize text data for analysis:
    - Convert to lowercase
    - Remove punctuation
    - Tokenize (split into words)
    - Remove stop words (optional)
    """
    print("\n" + "="*70)
    print("LESSON 1: Text Preprocessing")
    print("="*70)
    
    print("\n🔹 Original review texts:")
    reviews_df.select("review_id", "review_text").show(5, truncate=False)
    
    # Step 1: Convert to lowercase
    print("\n🔹 Step 1: Lowercase")
    reviews_clean = reviews_df.withColumn(
        "text_lower",
        lower(col("review_text"))
    )
    reviews_clean.select("review_text", "text_lower").show(3, truncate=False)
    
    # Step 2: Remove punctuation
    print("\n🔹 Step 2: Remove punctuation")
    reviews_clean = reviews_clean.withColumn(
        "text_clean",
        regexp_replace(col("text_lower"), "[^a-zA-Z0-9\\s]", "")
    )
    reviews_clean.select("text_lower", "text_clean").show(3, truncate=False)
    
    # Step 3: Tokenize (split into words)
    print("\n🔹 Step 3: Tokenization (split into words)")
    reviews_clean = reviews_clean.withColumn(
        "tokens",
        split(trim(col("text_clean")), "\\s+")
    )
    reviews_clean.select("text_clean", "tokens").show(3, truncate=False)
    
    # Step 4: Filter out common stop words (simplified)
    stop_words = ["the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", 
                  "of", "with", "is", "was", "it", "this", "that"]
    
    @udf(ArrayType(StringType()))
    def remove_stop_words(tokens):
        """Remove common stop words"""
        if tokens:
            return [word for word in tokens if word not in stop_words and len(word) > 2]
        return []
    
    print("\n🔹 Step 4: Remove stop words and short words")
    reviews_clean = reviews_clean.withColumn(
        "filtered_tokens",
        remove_stop_words(col("tokens"))
    )
    reviews_clean.select("tokens", "filtered_tokens").show(3, truncate=False)
    
    return reviews_clean


def inverted_index(reviews_df):
    """
    LESSON 2: Building an Inverted Index
    
    An inverted index maps each word to the documents (reviews) containing it.
    This is the foundation of search engines.
    
    Structure: word -> [list of document IDs]
    """
    print("\n" + "="*70)
    print("LESSON 2: Building an Inverted Index")
    print("="*70)
    
    print("\n🔹 Creating inverted index from reviews:")
    
    # Preprocess text
    reviews_processed = reviews_df \
        .withColumn("text_clean", lower(regexp_replace(col("review_text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("tokens", split(trim(col("text_clean")), "\\s+"))
    
    # Explode tokens to get one row per word per document
    word_document_pairs = reviews_processed \
        .select(
            col("review_id").alias("doc_id"),
            explode(col("tokens")).alias("word")
        ) \
        .filter(length(col("word")) > 2)  # Filter short words
    
    print("\nWord-Document pairs (sample):")
    word_document_pairs.show(10)
    
    # Build inverted index: group by word, collect document IDs
    inverted_idx = word_document_pairs \
        .groupBy("word") \
        .agg(
            collect_set("doc_id").alias("doc_ids"),
            count("doc_id").alias("doc_frequency")
        ) \
        .orderBy(desc("doc_frequency"))
    
    print("\n🔹 Inverted Index (top words):")
    print("Format: word -> [list of document IDs that contain it]")
    inverted_idx.show(20, truncate=False)
    
    # Search function using inverted index
    print("\n🔹 Search for 'great':")
    search_result = inverted_idx.filter(col("word") == "great")
    if search_result.count() > 0:
        docs = search_result.first()["doc_ids"]
        print(f"Found in {len(docs)} documents: {docs[:10]}")
    
    return inverted_idx


def term_frequency(reviews_df):
    """
    LESSON 3: Term Frequency (TF)
    
    TF measures how often a term appears in a document.
    Common formula: TF = (count of term in document) / (total terms in document)
    """
    print("\n" + "="*70)
    print("LESSON 3: Term Frequency (TF)")
    print("="*70)
    
    print("\n🔹 Calculating term frequency for each word in each document:")
    
    # Preprocess
    reviews_processed = reviews_df \
        .withColumn("text_clean", lower(regexp_replace(col("review_text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("tokens", split(trim(col("text_clean")), "\\s+")) \
        .withColumn("doc_length", size(col("tokens")))
    
    # Explode tokens
    word_doc_pairs = reviews_processed \
        .select("review_id", "doc_length", explode(col("tokens")).alias("word")) \
        .filter(length(col("word")) > 2)
    
    # Count term frequency per document
    tf = word_doc_pairs \
        .groupBy("review_id", "doc_length", "word") \
        .agg(count("*").alias("term_count")) \
        .withColumn("tf", col("term_count") / col("doc_length"))
    
    print("\nTerm Frequency (sample):")
    tf.orderBy(desc("tf")).show(15)
    
    return tf


def tfidf_scoring(reviews_df):
    """
    LESSON 4: TF-IDF (Term Frequency-Inverse Document Frequency)
    
    TF-IDF measures how important a word is to a document in a collection.
    
    Formula:
    - TF = term frequency in document
    - IDF = log(total documents / documents containing term)
    - TF-IDF = TF * IDF
    
    High TF-IDF = word is common in this doc but rare across all docs
    """
    print("\n" + "="*70)
    print("LESSON 4: TF-IDF Scoring")
    print("="*70)
    
    print("\n🔹 Calculating TF-IDF scores:")
    
    # Preprocess
    reviews_processed = reviews_df \
        .withColumn("text_clean", lower(regexp_replace(col("review_text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("tokens", split(trim(col("text_clean")), "\\s+")) \
        .withColumn("doc_length", size(col("tokens")))
    
    total_docs = reviews_processed.count()
    
    # Calculate TF
    word_doc_pairs = reviews_processed \
        .select("review_id", "doc_length", explode(col("tokens")).alias("word")) \
        .filter(length(col("word")) > 2)
    
    tf = word_doc_pairs \
        .groupBy("review_id", "doc_length", "word") \
        .agg(count("*").alias("term_count")) \
        .withColumn("tf", col("term_count") / col("doc_length"))
    
    # Calculate IDF (document frequency)
    df_counts = word_doc_pairs \
        .select("word", "review_id") \
        .distinct() \
        .groupBy("word") \
        .agg(count("*").alias("doc_frequency"))
    
    # Join TF with IDF
    from pyspark.sql.functions import log
    
    tfidf = tf.join(df_counts, "word") \
        .withColumn("idf", log(lit(total_docs) / col("doc_frequency"))) \
        .withColumn("tfidf", col("tf") * col("idf")) \
        .select("review_id", "word", "tf", "idf", "tfidf")
    
    print("\nTF-IDF Scores (sample - highest scores):")
    tfidf.orderBy(desc("tfidf")).show(20)
    
    # Top terms per document
    print("\n🔹 Top 5 important terms per document (by TF-IDF):")
    window = Window.partitionBy("review_id").orderBy(desc("tfidf"))
    
    top_terms = tfidf \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5) \
        .groupBy("review_id") \
        .agg(collect_list("word").alias("top_terms"))
    
    # Join with original reviews
    reviews_with_terms = reviews_df.join(top_terms, "review_id")
    reviews_with_terms.select("review_id", "review_text", "top_terms").show(10, truncate=False)
    
    return tfidf


def product_search_index(products_df):
    """
    LESSON 5: Building a Product Search Index
    
    Create a searchable index of products based on their attributes.
    This enables full-text search over product names, categories, and brands.
    """
    print("\n" + "="*70)
    print("LESSON 5: Product Search Index")
    print("="*70)
    
    print("\n🔹 Building searchable product index:")
    
    # Combine searchable fields
    products_searchable = products_df.withColumn(
        "search_text",
        lower(concat_ws(" ", col("name"), col("category"), col("brand")))
    )
    
    print("\nProducts with search text:")
    products_searchable.select("product_id", "name", "search_text").show(10, truncate=False)
    
    # Tokenize and create inverted index
    products_tokenized = products_searchable \
        .withColumn("tokens", split(col("search_text"), "\\s+"))
    
    # Build inverted index
    product_index = products_tokenized \
        .select(col("product_id"), explode(col("tokens")).alias("term")) \
        .filter(length(col("term")) > 2) \
        .groupBy("term") \
        .agg(collect_list("product_id").alias("product_ids"))
    
    print("\n🔹 Product search index (sample):")
    product_index.show(20, truncate=False)
    
    # Search function
    def search_products(search_term):
        """Search for products containing the term"""
        print(f"\n🔍 Searching for: '{search_term}'")
        results = product_index.filter(col("term") == search_term.lower())
        
        if results.count() > 0:
            product_ids = results.first()["product_ids"]
            print(f"Found {len(product_ids)} products")
            
            # Get product details
            matching_products = products_df.filter(col("product_id").isin(product_ids))
            matching_products.select("product_id", "name", "category", "brand").show(10, truncate=False)
        else:
            print("No products found")
    
    # Example searches
    search_products("electronics")
    search_products("brandA")
    
    return product_index


def product_recommendations(transactions_df, products_df):
    """
    LESSON 6: Product Recommendations (Collaborative Filtering - Simple)
    
    Build a simple recommendation system:
    - Find products frequently bought together
    - Recommend based on co-occurrence
    """
    print("\n" + "="*70)
    print("LESSON 6: Product Recommendations")
    print("="*70)
    
    print("\n🔹 Finding products frequently bought together:")
    
    # Get user purchase history
    user_products = transactions_df \
        .filter(col("status") == "completed") \
        .groupBy("user_id") \
        .agg(collect_list("product_id").alias("products"))
    
    print("\nUser purchase history (sample):")
    user_products.show(5, truncate=False)
    
    # Self-join to find product pairs bought by same users
    from pyspark.sql.functions import array_intersect, array_except, array_union
    
    # Count co-occurrences
    product_pairs = transactions_df \
        .filter(col("status") == "completed") \
        .select("user_id", "product_id") \
        .alias("t1") \
        .join(
            transactions_df.filter(col("status") == "completed")
            .select("user_id", "product_id")
            .alias("t2"),
            col("t1.user_id") == col("t2.user_id")
        ) \
        .filter(col("t1.product_id") < col("t2.product_id")) \
        .groupBy(col("t1.product_id").alias("product_a"), col("t2.product_id").alias("product_b")) \
        .agg(count("*").alias("co_occurrence_count")) \
        .orderBy(desc("co_occurrence_count"))
    
    print("\n🔹 Products frequently bought together:")
    product_pairs.show(20)
    
    # Enrich with product names
    product_pairs_enriched = product_pairs \
        .join(products_df.alias("pa"), col("product_a") == col("pa.product_id")) \
        .join(products_df.alias("pb"), col("product_b") == col("pb.product_id")) \
        .select(
            col("product_a"),
            col("pa.name").alias("product_a_name"),
            col("product_b"),
            col("pb.name").alias("product_b_name"),
            col("co_occurrence_count")
        ) \
        .orderBy(desc("co_occurrence_count"))
    
    print("\n🔹 Product recommendations (with names):")
    product_pairs_enriched.show(15, truncate=False)
    
    # Given a product, recommend others
    def recommend_for_product(product_id):
        """Recommend products based on co-purchases"""
        print(f"\n🎯 Recommendations for product {product_id}:")
        
        recommendations = product_pairs_enriched.filter(
            (col("product_a") == product_id) | (col("product_b") == product_id)
        )
        
        recommendations.show(10, truncate=False)
    
    # Example
    recommend_for_product(1)


def text_similarity(reviews_df):
    """
    LESSON 7: Text Similarity
    
    Find similar documents based on shared terms.
    Useful for duplicate detection, clustering, and recommendations.
    """
    print("\n" + "="*70)
    print("LESSON 7: Text Similarity")
    print("="*70)
    
    print("\n🔹 Finding similar reviews based on word overlap:")
    
    # Preprocess and tokenize
    reviews_processed = reviews_df \
        .withColumn("text_clean", lower(regexp_replace(col("review_text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("tokens", split(trim(col("text_clean")), "\\s+")) \
        .select("review_id", "review_text", "tokens")
    
    # Self-join to compare all pairs
    similarity = reviews_processed.alias("r1").join(
        reviews_processed.alias("r2"),
        col("r1.review_id") < col("r2.review_id")
    )
    
    # Calculate Jaccard similarity: |A ∩ B| / |A ∪ B|
    @udf(FloatType())
    def jaccard_similarity(tokens1, tokens2):
        """Calculate Jaccard similarity between two token lists"""
        if not tokens1 or not tokens2:
            return 0.0
        
        set1 = set(tokens1)
        set2 = set(tokens2)
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return float(intersection) / float(union) if union > 0 else 0.0
    
    similarity_scores = similarity \
        .withColumn("similarity", jaccard_similarity(col("r1.tokens"), col("r2.tokens"))) \
        .filter(col("similarity") > 0.3) \
        .select(
            col("r1.review_id").alias("review_1"),
            col("r1.review_text").alias("text_1"),
            col("r2.review_id").alias("review_2"),
            col("r2.review_text").alias("text_2"),
            col("similarity")
        ) \
        .orderBy(desc("similarity"))
    
    print("\nSimilar reviews (similarity > 0.3):")
    similarity_scores.show(10, truncate=False)


def main():
    """
    Main function - orchestrates all lessons.
    """
    print("\n" + "🎓 " + "="*66 + " 🎓")
    print("   JOB 5: Search Indexing and Text Processing")
    print("🎓 " + "="*66 + " 🎓\n")
    
    spark = get_spark_session("Job 5: Search Indexing")
    data_dir = get_data_dir()
    
    try:
        # Ensure data exists
        reviews_csv = os.path.join(data_dir, "reviews.csv")
        if not os.path.exists(reviews_csv):
            print("📁 Sample data not found. Generating...")
            generate_all_datasets(data_dir)
        
        # Load data
        print("\n📊 Loading datasets...")
        reviews_df = spark.read.csv(reviews_csv, header=True, inferSchema=True)
        products_df = spark.read.json(os.path.join(data_dir, "products.json"))
        transactions_df = spark.read.csv(
            os.path.join(data_dir, "transactions.csv"),
            header=True,
            inferSchema=True
        )
        
        print(f"✓ Loaded {reviews_df.count()} reviews")
        print(f"✓ Loaded {products_df.count()} products")
        print(f"✓ Loaded {transactions_df.count()} transactions")
        
        # Run lessons
        reviews_clean = text_preprocessing(reviews_df)
        inverted_idx = inverted_index(reviews_df)
        tf = term_frequency(reviews_df)
        tfidf = tfidf_scoring(reviews_df)
        product_idx = product_search_index(products_df)
        product_recommendations(transactions_df, products_df)
        text_similarity(reviews_df)
        
        # Final summary
        print("\n" + "="*70)
        print("✅ JOB 5 COMPLETED!")
        print("="*70)
        print("\n📚 What you learned:")
        print("  ✓ Text preprocessing and tokenization")
        print("  ✓ Building inverted indexes")
        print("  ✓ Term Frequency (TF) calculation")
        print("  ✓ TF-IDF scoring for relevance")
        print("  ✓ Product search indexes")
        print("  ✓ Simple recommendation systems")
        print("  ✓ Text similarity with Jaccard coefficient")
        print("\n🎉 Congratulations! You've completed all PySpark learning jobs!")
        print("\n📖 Next steps:")
        print("  • Review docs/concepts.md for deeper understanding")
        print("  • Modify the jobs to experiment with different patterns")
        print("  • Apply these patterns to your own datasets")
        print("  • Explore PySpark ML library for machine learning")
        print("  • Check out Spark UI (http://localhost:4040) while jobs run")
        print("\n💡 Keep building and learning!\n")
        
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
