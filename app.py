from transformers import pipeline
from confluent_kafka import Producer, Consumer, KafkaError
import uuid
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
REQUEST_TOPIC = 'translationEnToArabicRequest'
RESPONSE_TOPIC = 'translationEnToArabicResponse'

# Kafka Producer Configuration
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'translation_service_worker',
    'auto.offset.reset': 'earliest',
}

# Load Translation Pipeline
def load_translation_pipeline():
    try:
        return pipeline(task='translation_en_to_ar', model='Helsinki-NLP/opus-mt-en-ar')
    except Exception as e:
        print(f"[Pipeline Error] Could not load translation pipeline: {e}")
        exit(1)

# Initialize translation pipeline
print("[Pipeline] Loading translation pipeline...")
translation_pipeline = load_translation_pipeline()
print("[Pipeline] Translation pipeline loaded.")

def kafka_produce_response(job_id, translated_text):
    """Produce the translation result to the response topic."""
    response_data = {
        "id": job_id,
        "translated_text": translated_text,
        "status": "completed!"
    }
    try:
        producer.produce(RESPONSE_TOPIC, json.dumps(response_data))
        producer.flush()
        print(f"[Producer] Sent translation result for job {job_id} to {RESPONSE_TOPIC}")
    except Exception as e:
        print(f"[Producer Error] Failed to send response: {e}")

def kafka_consumer_worker():
    """Consume translation requests from Kafka."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([REQUEST_TOPIC])
    print("[Consumer] Kafka Consumer Worker Started...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[Consumer Error] {msg.error()}")
                    break

            # Process the incoming request
            try:
                request_data = json.loads(msg.value().decode('utf-8'))
                print(f"[Consumer] Received request: {request_data}")

                # Extract request data
                job_id = request_data.get("id", str(uuid.uuid4()))
                text = request_data.get("text", "")
                if not text:
                    print(f"[Consumer Error] Empty text in request: {request_data}")
                    continue

                # Perform translation
                translated = translation_pipeline(text)[0]['translation_text']
                print(f"[Consumer] Translated text: {translated}")

                # Produce the response to the response topic
                kafka_produce_response(job_id, translated)
            except json.JSONDecodeError:
                print(f"[Consumer Error] Invalid message format: {msg.value()}")
            except Exception as e:
                print(f"[Consumer Error] Failed to process message: {e}")
    finally:
        consumer.close()
        print("[Consumer] Kafka Consumer Worker Stopped.")

if __name__ == "__main__":
    kafka_consumer_worker()