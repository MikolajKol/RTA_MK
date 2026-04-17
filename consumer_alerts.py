from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'alerts',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='alerts-viewer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("=== URUCHOMIONO PODGLĄD ALERTÓW ===")
print(f"{'TX_ID':<8} | {'SCORE':<5} | {'RULES':<10} | {'AMOUNT':<10}")
print("-" * 45)

try:
    for message in consumer:
        alert = message.value
        tx_id = alert.get('tx_id', 'N/A')
        score = alert.get('score', 0)
        rules = ", ".join(alert.get('rules', []))
        amount = alert.get('amount', 0.0)
        
        print(f"{tx_id:<8} | {score:<5} | {rules:<10} | {amount:<10.2f} PLN")
except KeyboardInterrupt:
    print("\nZatrzymano podgląd.")
finally:
    consumer.close()
