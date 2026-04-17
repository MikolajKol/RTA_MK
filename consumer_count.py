from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx.get('store', 'Nieznany')
    amount = tx.get('amount', 0.0)

    # 1. Zliczanie transakcji
    store_counts[store] += 1
    # 2. Sumowanie kwot
    total_amount[store] += amount
    
    msg_count += 1

    # 3. Co 10 wiadomości: wypisz tabelę
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (Odebrano: {msg_count}) ---")
        print(f"{'Sklep':<15} | {'Liczba TX':<10} | {'Suma PLN':<12}")
        print("-" * 43)
        for s in sorted(store_counts.keys()):
            print(f"{s:<15} | {store_counts[s]:<10} | {total_amount[s]:>10.2f}")
        print("-" * 43)

