import json, time
from kafka import KafkaProducer

def json_serializer(d): return json.dumps(d).encode("utf-8")
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=json_serializer,
    acks="all",
    linger_ms=5,
    retries=3
)
topic = "team.watch"
for i in range(10):
    payload = {
        "event": "watch",
        "user_id": f"user_{i%3}",
        "movie_id": f"m_{100+i}",
        "minute": (i % 5) + 1,
        "ts": time.time()
}
    team_watch = payload["team.watch"].encode()
    md = producer.send(topic, value=payload, key=team_watch).get(timeout=10)
topic = "team.rate"
for i in range(10):
    payload = {
        "event": "rating",
        "user_id": f"user_{i%3}",
        "movie_id": f"m_{100+i}",
        "rating": (i % 5) + 1,
        "ts": time.time()
}
    team_rate = payload["team.rate"].encode()
    md = producer.send(topic, value=payload, key=team_rate).get(timeout=10)
    print(f"Sent -> partition {md.partition}, offset {md.offset}")
topic = "team.reco_requests"
print("Sending 10 test messages...")
for i in range(10):
    payload = {
        "event": "API requests
        key = payload["user_id"].encode()
    md = producer.send(topic, value=payload, key=key).get(timeout=10)
    print(f"Sent -> partition {md.partition}, offset {md.offset}")
producer.flush(); producer.close()
print("Done.")
