import time
import mysql.connector
import sys
import os

# Database connection properties
DB_HOST = "localhost"
DB_USER = "sparkuser"
DB_PASS = "Spark@123"
DB_NAME = "tweetdb"
DB_TABLE = "scored_tweets"  # updated table name

try:
    start_time = time.time()

    print("Waiting 30 seconds for streaming mode to populate MySQL...")
    wait_time = 30

    # Show progress bar
    for i in range(wait_time):
        progress = int((i / wait_time) * 20)
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('=' * progress, 5 * progress))
        sys.stdout.flush()
        time.sleep(1)

    print("\nWait completed. Retrieving results...")

    # Connect to MySQL and query results
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Count total tweets
        cursor.execute(f"SELECT COUNT(*) FROM {DB_TABLE}")
        total_count = cursor.fetchone()[0]

        if total_count == 0:
            print("No data found in database. Make sure the streaming process is running.")
            sys.exit(1)

        print(f"\nTotal tweets analyzed: {total_count}")

        # Get sentiment distribution and average polarity
        cursor.execute(f"""
            SELECT sentiment, COUNT(*) AS tweet_count, AVG(polarity) AS average_polarity
            FROM {DB_TABLE}
            GROUP BY sentiment
            ORDER BY tweet_count DESC;
        """)
        results = cursor.fetchall()

        print("\n=== STREAMING MODE RESULTS ===")
        print("Sentiment   | Count | Avg Polarity")
        print("-" * 40)
        for row in results:
            sentiment, count, avg_polarity = row
            print(f"{sentiment:11} | {count:5} | {avg_polarity:.4f}")

        # Display percentage distribution
        print("\nSentiment Distribution (%):")
        for row in results:
            sentiment, count, _ = row
            percentage = (count / total_count) * 100
            print(f"{sentiment:11}: {percentage:.2f}%")

        end_time = time.time()
        print(f"\nStreaming Mode Evaluation Time: {end_time - start_time:.2f} seconds")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        print("Make sure MySQL is running and the table exists.")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

