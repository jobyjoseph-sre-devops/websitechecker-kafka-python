from kafka_producer import WebsiteKafkaProducer
from kafka_consumer import WebsiteKafkaConsumer
import time

# Monitor website periodically
while True:
    # Produce Website Metrics
    try:
        website_url = "https://google.com"
        website_search_str = "Gmail"
        website_producer = WebsiteKafkaProducer(
            website_url, website_search_str)
        website_producer.metrics_from_website_to_topic()
        print("success")
    except Exception as e:
        print(e)

    # Consomes  Website Metrics
    try:
        website_consumer = WebsiteKafkaConsumer()
        website_consumer.messages_from_topic_to_database()
        print("success")
    except Exception as e:
        print(e)

    time.sleep(60)
