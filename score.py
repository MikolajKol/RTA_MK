from datetime import datetime

def score_transaction(tx):
    score = 0
    rules = []
    
    # R1: kwota > 3000 (+3)
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
    
    # R2: elektronika i kwota > 1500 (+2)
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
    
    # R3: godzina < 6 (noc) (+2) przy użyciu biblioteki datetime
    dt = datetime.fromisoformat(tx['timestamp'])
    if dt.hour < 6:
        score += 2
        rules.append('R3')

    status = "PODEJRZANA" if score >= 3 else "OK"
        
    return score, rules, status

# Test
test_tx = {
    'tx_id': 'TX999', 
    'amount': 4500.0, 
    'category': 'elektronika',
    'timestamp': '2026-04-01T03:15:00'
}

print(score_transaction(test_tx))