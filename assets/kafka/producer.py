#!/usr/bin/env python3
"""
Producer Kafka — Simulateur de transactions bancaires
Dataset : Kaggle Credit Card Fraud (500 tx/sec)
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Garantie de réception
)

# Données de simulation
COUNTRIES = ['MA', 'FR', 'US', 'UK', 'IR', 'KP', 'CH', 'SY', 'SOM']
MERCHANTS = ['Amazon', 'Carrefour', 'Shell', 'SPA', 'Jumia', 'Cora', 'MARJANE', 'BNP Paribas']
CARD_TYPES = ['VISA', 'MASTERCARD', 'AMEX', 'DISCOVER']

def generate_transaction():
    """
    Génère une transaction bancaire simulée.
    Basé sur le dataset Kaggle Credit Card Fraud (montants réalistes)
    """
    return {
        'tx_id': f"TXN_{int(time.time()*1000)}_{random.randint(1000, 9999)}",
        'amount': round(random.uniform(100, 50000), 2),  # 100 à 50k MAD
        'country': random.choice(COUNTRIES),
        'merchant': random.choice(MERCHANTS),
        'card_type': random.choice(CARD_TYPES),
        'timestamp': datetime.utcnow().isoformat(),
        'is_fraud': random.random() < 0.02,  # 2% fraud rate (Kaggle dataset)
        'cardholder_age': random.randint(18, 75),
    }

def send_transactions(rate_per_sec=500, duration_minutes=10):
    """
    Envoie des transactions à 500 tx/sec.
    
    Args:
        rate_per_sec: nombre de transactions par seconde (défaut 500)
        duration_minutes: durée totale en minutes (défaut 10)
    """
    batch_size = 50  # 50 envois par batch
    delay_between_batches = batch_size / rate_per_sec  # délai en secondes
    
    print(f"🚀 Producer Kafka démarré")
    print(f"📊 Target: {rate_per_sec} tx/sec")
    print(f"⏱️  Durée: {duration_minutes} minutes")
    print(f"📍 Topic: transactions")
    print(f"⏹️  Appuyez sur Ctrl+C pour arrêter\n")
    
    try:
        tx_count = 0
        start_time = time.time()
        
        while True:
            batch_start = time.time()
            
            # Envoyer batch_size transactions
            for _ in range(batch_size):
                tx = generate_transaction()
                
                # Envoyer et attendre confirmation
                future = producer.send('transactions', value=tx)
                
                def on_error(exc):
                    print(f"❌ Erreur d'envoi: {exc}")
                
                future.add_errback(on_error)
                tx_count += 1
            
            # Rate limiting : respecter le débit
            elapsed_since_batch = time.time() - batch_start
            target_delay = delay_between_batches
            
            if elapsed_since_batch < target_delay:
                time.sleep(target_delay - elapsed_since_batch)
            
            # Afficher stats tous les 500 messages
            if tx_count % 500 == 0:
                elapsed_total = time.time() - start_time
                actual_rate = tx_count / elapsed_total if elapsed_total > 0 else 0
                print(f"✅ {tx_count} tx envoyées | {elapsed_total:.1f}s écoulées | "
                      f"Débit réel: {actual_rate:.0f} tx/sec")
            
            # Vérifier durée limite
            if (time.time() - start_time) > (duration_minutes * 60):
                print(f"\n⏰ Durée limite atteinte ({duration_minutes} min)")
                break
                
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Arrêt manuel")
    finally:
        print(f"\n📊 RÉSUMÉ FINAL")
        print(f"├── Total transactions: {tx_count}")
        print(f"├── Durée: {time.time() - start_time:.1f}s")
        print(f"└── Débit moyen: {tx_count / (time.time() - start_time):.0f} tx/sec")
        
        # Flush et fermeture
        producer.flush()
        producer.close()
        print("✅ Producer fermé proprement")

if __name__ == '__main__':
    # Lancer le producer
    # rate_per_sec=500: 500 transactions par seconde
    # duration_minutes=10: s'arrête automatiquement après 10 minutes
    send_transactions(rate_per_sec=500, duration_minutes=10)
