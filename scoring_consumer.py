from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# --- FUNKCJE ---

def score_transaction(tx):
    score = 0
    rules = []
    
    # R1: amount > 3000 (+3)
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1')
        
    # R2: elektronika i amount > 1500 (+2)
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2')
        
    # R3: godzina < 6 (+2)
    dt = datetime.fromisoformat(tx['timestamp'])
    if dt.hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

def get_status(score):
    return "PODEJRZANA" if score >= 3 else "OK"

# --- PĘTLA GŁÓWNA ---

print("Nasłuchiwanie transakcji...")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    status = get_status(score)
    
    if score >= 3:
        # Dodanie pól do transakcji przed wysyłką
        tx['score'] = score
        tx['rules'] = rules
        tx['status'] = status
        
        # Wysłanie do topiku 'alerts'
        alert_producer.send('alerts', value=tx)
        
        # Wypisanie wyniku
        print(f"ALERT: {tx['tx_id']} | Status: {status} | Punkty: {score} | Reguły: {rules}")
