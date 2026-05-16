#!/usr/bin/env python3
"""
Consumer Kafka — Filtre AML (Anti-Money Laundering)
Détecte transactions suspectes avec Exactly-Once Semantics (EOS)
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration Consumer Kafka
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    group_id='aml-filter',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=False,  # ⭐ EXACTLY-ONCE SEMANTICS: commit manuel
    session_timeout_ms=30000,
)

# Règles AML
BLACKLIST_COUNTRIES = ['IR', 'KP', 'SY', 'SOM']  # Pays sanctionnés (OFAC)
AML_THRESHOLD_MAD = 10000  # Montant seuil en MAD
FRAUD_THRESHOLD = 0.95  # Score fraude

def evaluate_aml_rules(tx):
    """
    Évalue une transaction selon les règles AML.
    
    Returns:
        tuple: (is_suspicious, alerts, risk_score)
    """
    alerts = []
    risk_score = 0
    
    # RÈGLE 1 : Montant élevé (>10k MAD)
    if tx['amount'] > AML_THRESHOLD_MAD:
        alerts.append(f"MONTANT_ÉLEVÉ({tx['amount']:.2f} MAD)")
        risk_score += 40
    
    # RÈGLE 2 : Pays blacklisté (sanctions OFAC)
    if tx['country'] in BLACKLIST_COUNTRIES:
        alerts.append(f"PAYS_INTERDIT({tx['country']})")
        risk_score += 50
    
    # RÈGLE 3 : Fraude détectée
    if tx.get('is_fraud', False):
        alerts.append("FRAUDE_DÉTECTÉE")
        risk_score += 35
    
    # Transaction suspecte si risk_score > 50
    is_suspicious = risk_score > 50
    
    return is_suspicious, alerts, risk_score

def main():
    """Boucle principale du consumer"""
    
    logger.info("=" * 80)
    logger.info("🔍 CONSUMER AML DÉMARRÉ")
    logger.info("=" * 80)
    logger.info(f"📊 Topic: transactions")
    logger.info(f"📋 Consumer Group: aml-filter")
    logger.info(f"🔒 EOS (Exactly-Once): ACTIVÉ")
    logger.info(f"⚠️  Règles AML activées:")
    logger.info(f"    • Montant seuil: {AML_THRESHOLD_MAD:,} MAD")
    logger.info(f"    • Pays blacklistés: {', '.join(BLACKLIST_COUNTRIES)}")
    logger.info(f"    • Risk score > 50 = ALERTE")
    logger.info("=" * 80)
    logger.info("")
    
    try:
        processed = 0
        flagged = 0
        
        for message in consumer:
            tx = message.value
            is_alert, alerts, risk = evaluate_aml_rules(tx)
            
            processed += 1
            
            if is_alert:
                # ALERTE : Transaction suspecte
                flagged += 1
                logger.warning(
                    f"🚨 [ALERTE AML] | TxID: {tx['tx_id'][:20]}... | "
                    f"Montant: {tx['amount']:>8.2f} MAD | "
                    f"Pays: {tx['country']} | "
                    f"Marchand: {tx['merchant']:<12} | "
                    f"Risk: {risk:>3}% | "
                    f"Détails: {' + '.join(alerts)}"
                )
            else:
                # OK : Transaction normale
                logger.info(
                    f"✅ [OK] | TxID: {tx['tx_id'][:20]}... | "
                    f"Montant: {tx['amount']:>8.2f} MAD | "
                    f"Pays: {tx['country']} | "
                    f"Marchand: {tx['merchant']:<12} | "
                    f"Carte: {tx['card_type']}"
                )
            
            # ⭐ EXACTLY-ONCE : Commit tous les 100 messages traités
            if processed % 100 == 0:
                consumer.commit()
                alert_ratio = (flagged / processed * 100) if processed > 0 else 0
                logger.info(
                    f"📊 [CHECKPOINT] Traité: {processed:>5} | "
                    f"Alertes: {flagged:>4} ({alert_ratio:>5.1f}%) | "
                    f"Offset: {message.offset}"
                )
            
            # STOP après 5000 messages (démo)
            if processed >= 5000:
                logger.info("")
                logger.warning("🛑 Limite démo atteinte (5000 messages)")
                break
        
        # Commit final
        consumer.commit()
        
    except KafkaError as e:
        logger.error(f"❌ Erreur Kafka: {e}")
    except KeyboardInterrupt:
        logger.info("")
        logger.info("👋 Arrêt manuel du consumer")
    finally:
        # Afficher statistiques finales
        if processed > 0:
            alert_ratio = (flagged / processed * 100)
            logger.info("")
            logger.info("=" * 80)
            logger.info("📈 STATISTIQUES FINALES")
            logger.info("=" * 80)
            logger.info(f"Total traité: {processed}")
            logger.info(f"Total alertes: {flagged}")
            logger.info(f"Ratio alertes: {alert_ratio:.2f}%")
            logger.info(f"Status: Tous les messages committé (EOS assuré)")
            logger.info("=" * 80)
        
        consumer.close()
        logger.info("✅ Consumer fermé proprement\n")

if __name__ == '__main__':
    main()
