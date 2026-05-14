# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 — Big Data en pratique : Hadoop, MapReduce & Spark
# MAGIC ## Master Finance Digitale ISCE Casablanca
# MAGIC 
# MAGIC **Durée :** 45 minutes | **Format :** hands-on sur Databricks Community Edition  
# MAGIC **Objectif :** comprendre MapReduce via Spark, appliquer à un cas finance réel (concentration sectorielle Bâle IV)
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 : Concepts fondamentaux (5 min lecture + code)
# MAGIC 
# MAGIC ### MapReduce pattern : diviser pour régner
# MAGIC 
# MAGIC **MapReduce = 3 phases :**
# MAGIC 1. **MAP** : transformer chaque élément isolément
# MAGIC 2. **SHUFFLE** : regrouper par clé (automatique dans Spark)
# MAGIC 3. **REDUCE** : agréger les valeurs par clé
# MAGIC 
# MAGIC **Exemple simple : compter les mots**
# MAGIC 
# MAGIC | Phase | Opération | Exemple |
# MAGIC |-------|-----------|---------|
# MAGIC | **MAP** | Transformer chaque mot en (mot, 1) | "hadoop" → ("hadoop", 1) |
# MAGIC | **SHUFFLE** | Regrouper par mot | [("hadoop", 1), ("hadoop", 1)] → tous les "hadoop" ensemble |
# MAGIC | **REDUCE** | Additionner les 1 pour chaque mot | ("hadoop", [1, 1, 1]) → ("hadoop", 3) |
# MAGIC 
# MAGIC ### Pourquoi c'est du Big Data ?
# MAGIC - **Distribuable** : chaque nœud traite sa portion en parallèle
# MAGIC - **Scalable** : 1 Go sur 1 machine = 1 To sur 100 machines
# MAGIC - **Tolérant aux pannes** : si un nœud échoue, ses données sont répliquées ailleurs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 : Word Count — Le "Hello World" du Big Data
# MAGIC 
# MAGIC Exécutez chaque cellule ci-dessous. Vous allez voir MapReduce en action.

# COMMAND ----------

# Données : liste de mots (simulation d'un texte énorme)
words = ["hadoop", "mapreduce", "spark", "hadoop", "spark", "hadoop", 
         "spark", "spark", "mapreduce", "hadoop", "databricks"]

# Créer un RDD Spark distribué
rdd_words = sc.parallelize(words)

print("✅ Données en mémoire distribuée (RDD)")
print(f"Nombre total de mots : {rdd_words.count()}")
print(f"Premiers 5 mots : {rdd_words.take(5)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 1 : MAP — transformer en (clé, valeur)

# COMMAND ----------

# MAP phase : chaque mot devient (mot, 1)
mapped = rdd_words.map(lambda word: (word, 1))

print("Après MAP (premier 5) :")
for item in mapped.take(5):
    print(f"  {item}")
# Résultat attendu : ("hadoop", 1), ("mapreduce", 1), ("spark", 1), etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 2 : REDUCE — agréger par clé

# COMMAND ----------

# REDUCE phase : pour chaque mot, additionner les 1
word_counts = mapped.reduceByKey(lambda a, b: a + b)

print("✅ Résultat final (MapReduce complet) :")
results = sorted(word_counts.collect(), key=lambda x: x[1], reverse=True)
for word, count in results:
    print(f"  {word:15} → {count} occurrence(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résumé Section 2
# MAGIC 
# MAGIC Ce que vous venez de faire :
# MAGIC 
# MAGIC 1. **MAP** : transformé 11 mots en 11 paires (mot, 1)
# MAGIC 2. **SHUFFLE** : Spark a automatiquement regroupé les mots identiques
# MAGIC 3. **REDUCE** : somme les compteurs par mot
# MAGIC 
# MAGIC **C'est exactement ce que fait Hadoop MapReduce**, mais :
# MAGIC - Hadoop le fait avec un framework complexe (HDFS, JobTracker, TaskTracker)
# MAGIC - Spark le fait avec une abstraction plus simple et rapide (en mémoire, lazy evaluation)
# MAGIC 
# MAGIC **Vitesse :** Spark est ~10× plus rapide que Hadoop pour ce pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3 : Cas réel finance — Concentration sectorielle (Bâle IV)
# MAGIC 
# MAGIC **Contexte réglementaire :**  
# MAGIC Une banque doit calculer sa concentration de risque par secteur d'activité.  
# MAGIC Bâle IV impose un seuil maximum : la somme des encours d'un secteur ne doit pas dépasser X% du portefeuille total.
# MAGIC 
# MAGIC **Données :** 100 000 lignes de crédit (échantillon)  
# MAGIC **Question :** quel secteur concentre le plus de risque ?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 1 : Créer les données de simulation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round
import random

# Secteurs économiques d'une banque marocaine
sectors = ["Immobilier", "Commerce", "Industrie", "Tourisme", "Énergie", 
           "Télécoms", "Santé", "Agroalimentaire", "Transport", "Finance"]

# Simuler 100 000 crédits avec montants réalistes
# Structure : (client_id, secteur, montant_usd)
credits_data = []
for i in range(100000):
    sector = random.choice(sectors)
    montant = random.randint(10000, 5000000)  # 10k à 5M USD
    credits_data.append((f"client_{i}", sector, montant))

# Créer un DataFrame Spark (équivalent table SQL dans Data Warehouse)
df_credits = spark.createDataFrame(credits_data, ["client_id", "sector", "amount_usd"])

print("✅ Données de crédit chargées")
print(f"Nombre de crédits : {df_credits.count()}")
print(f"Montant total portefeuille : ${df_credits.agg(sum('amount_usd')).collect()[0][0]:,.0f}")

# COMMAND ----------

# Afficher les 5 premiers crédits
display(df_credits.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 2 : MapReduce pattern — agréger par secteur

# COMMAND ----------

# C'est ici que MapReduce fonctionne :
# - MAP : chaque crédit reste un crédit
# - SHUFFLE : regrouper par secteur
# - REDUCE : sommer les montants par secteur

sector_exposure = (df_credits
    .groupBy("sector")
    .agg(
        sum("amount_usd").alias("total_exposure"),
        count("*").alias("num_credits")
    )
)

# Ordonner par exposition décroissante
sector_exposure_sorted = sector_exposure.orderBy("total_exposure", ascending=False)

print("✅ Concentration par secteur (MapReduce appliqué) :")
display(sector_exposure_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 3 : Analyse Bâle IV — limite de concentration

# COMMAND ----------

from pyspark.sql.functions import col

# Calculer le montant total du portefeuille
total_portfolio = df_credits.agg(sum("amount_usd")).collect()[0][0]

# Ajouter le pourcentage de concentration
sector_exposure_pct = (sector_exposure_sorted
    .withColumn("concentration_pct", round((col("total_exposure") / total_portfolio) * 100, 2))
)

print(f"\n📊 Portefeuille total : ${total_portfolio:,.0f}")
print(f"⚠️  Limite Bâle IV typique : max 25% par secteur\n")

display(sector_exposure_pct)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Étape 4 : Identifier les secteurs en risque

# COMMAND ----------

# Seuil Bâle IV
BASEL_IV_LIMIT = 25.0

at_risk = (sector_exposure_pct
    .filter(col("concentration_pct") > BASEL_IV_LIMIT)
    .select("sector", "concentration_pct")
)

num_at_risk = at_risk.count()

print(f"\n🚨 Secteurs DÉPASSANT la limite Bâle IV ({BASEL_IV_LIMIT}%) :\n")

if num_at_risk == 0:
    print("✅ Aucun secteur en risque de concentration")
else:
    display(at_risk)
    print(f"\n⚠️  {num_at_risk} secteur(s) doit/doivent être réorienté(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4 : Approfondir — Croiser avec les données client

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cas avancé : concentration client au sein d'un secteur
# MAGIC 
# MAGIC Question : dans le secteur Immobilier, quel est le client qui concentre le plus de risque ?

# COMMAND ----------

# Filtrer un secteur à risque (le premier par concentration)
top_sector = sector_exposure_pct.orderBy("concentration_pct", ascending=False).first()["sector"]

print(f"Analyse du secteur : {top_sector}\n")

# Trouver les top 5 clients dans ce secteur
top_clients_in_sector = (df_credits
    .filter(col("sector") == top_sector)
    .groupBy("client_id")
    .agg(sum("amount_usd").alias("exposure"))
    .orderBy("exposure", ascending=False)
    .limit(5)
)

print(f"Top 5 clients dans le secteur {top_sector} :")
display(top_clients_in_sector)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résumé de l'atelier
# MAGIC 
# MAGIC #### Ce que vous avez appris :
# MAGIC 
# MAGIC | Concept | Implémentation Hadoop | Implémentation Spark (Databricks) |
# MAGIC |---------|----------------------|----------------------------------|
# MAGIC | **MAP** | Java, entrée/sortie fichier | `map()` ou `groupBy()` |
# MAGIC | **SHUFFLE** | JobTracker distribue | Automatique via catalyst optimizer |
# MAGIC | **REDUCE** | Combinaison / réduction | `reduceByKey()` ou `agg()` |
# MAGIC | **Performance** | Lent (disque) | Rapide (mémoire) |
# MAGIC | **Code** | ~100 lignes Java | ~5 lignes Python/Scala |
# MAGIC 
# MAGIC #### Application finance :
# MAGIC - ✅ Calculé la concentration de risque par secteur (100k crédits)
# MAGIC - ✅ Identifié les secteurs dépassant Bâle IV
# MAGIC - ✅ Analysé les clients concentrés au sein d'un secteur
# MAGIC 
# MAGIC #### Takeaway clé :
# MAGIC **Spark = MapReduce moderne.** Vous pouvez faire en 5 lignes de Python ce qui prenait 100 lignes de Java Hadoop, et c'est 10× plus rapide.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📚 Ressources & suite
# MAGIC 
# MAGIC **Ce notebook montre :**
# MAGIC - Word count (concept académique)
# MAGIC - Agrégation finance (cas réel Bâle IV)
# MAGIC - Pattern MapReduce appliqué à des données massives
# MAGIC 
# MAGIC **Pour aller plus loin (Jour 2) :**
# MAGIC - Session 5 (Spark) : optimisations, partitioning, caching
# MAGIC - Session 6 (ML) : entraîner un modèle de scoring sur Spark MLlib
# MAGIC - Session 7 (Gouvernance) : traçabilité et conformité Bâle IV
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Questions ?** Demandez à votre formateur.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus : Performance — comparez avec une boucle naïve

# COMMAND ----------

# MAGIC %md
# MAGIC **Combien de temps prendrait une boucle Python classique ?**
# MAGIC (Démonstration)

# COMMAND ----------

import time

# Approche naïve : boucle Python (lente)
start = time.time()

sector_totals_naive = {}
for client, sector, amount in credits_data:
    if sector not in sector_totals_naive:
        sector_totals_naive[sector] = 0
    sector_totals_naive[sector] += amount

naive_time = time.time() - start

print(f"⏱️  Boucle Python naïve : {naive_time:.2f}s")

# Approche Spark (rapide)
start = time.time()

spark_result = (df_credits
    .groupBy("sector")
    .agg(sum("amount_usd"))
    .collect()
)

spark_time = time.time() - start

print(f"⚡ Spark MapReduce : {spark_time:.4f}s")
print(f"\n🚀 Spark est {naive_time/spark_time:.0f}× plus rapide sur 100k lignes")
print(f"   (Sur 1 milliard de lignes, la différence serait encore plus grande)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC **Lab 2 terminé !**  
# MAGIC Prêt pour le Jour 2 : Spark SQL avancé, Machine Learning et Gouvernance.
