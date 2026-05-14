# Guide d'utilisation — Lab 2 Databricks (Hadoop, MapReduce, Spark)

## Pour le formateur

### Étape 1 : Créer un compte Databricks Community (si ce n'est pas déjà fait)
1. Aller sur https://databricks.com/community/profile/login
2. Cliquer **"Get started with Community Edition"**
3. Email + password → vérification email
4. Workspace lancé automatiquement

### Étape 2 : Importer le Notebook Lab 2

#### Option A : Importer depuis un fichier (recommandé)
1. Dans Databricks, cliquer sur le logo **"Databricks"** (haut-gauche)
2. **Workspace** (menu gauche) → **Users** → votre email
3. Cliquer sur **"Import"** (ou ⬆️ en haut à droite)
4. Choisir **"File"** → sélectionner le fichier `Lab2_Hadoop_Spark_Finance.py`
5. Cliquer **"Import"**

#### Option B : Créer manuellement et copier le code
1. **New** → **Notebook**
2. Nommer : `Lab 2 - Hadoop, MapReduce & Spark Finance`
3. Langage : **Python**
4. Copier-coller tout le contenu du fichier `.py` dans le notebook

### Étape 3 : Configurer un cluster

1. En haut du notebook, cliquer sur **"Compute"** (dropdown)
2. Cliquer sur **"Create compute"**
3. Configuration recommandée :
   - **Cluster name** : `Lab2-Cluster`
   - **Cluster mode** : Single Node (gratuit)
   - **Databricks Runtime** : la dernière version (ex : 13.3 LTS)
   - **Node type** : i3.xlarge (par défaut, c'est bon pour la démo)
4. Cliquer **"Create compute"**
5. Attendre le lancement (~2-3 min)

### Étape 4 : Vérifier que tout fonctionne

1. Dans le notebook, cliquer sur la première cellule de code (Section 1)
2. Cliquer **"Run cell"** (flèche ▶️)
3. Vérifier que aucune erreur n'apparaît

---

## Pour les participants

### Avant l'atelier (prendre 5 min)

1. **Créer un compte Databricks Community**
   - https://databricks.com/community/profile/login
   - "Get started" → email + password → confirmation email
   - **Voilà, vous avez votre workspace gratuit !**

2. **Attendre le lien du formateur** (il vous l'enverra le jour J ou avant)

### Jour de l'atelier

1. **Le formateur vous partage un lien** : quelque chose comme `https://databricks.com/notebook/.../Lab-2-...`

2. **Cliquer sur le lien** → vous êtes dans le notebook du formateur

3. **Cliquer sur "Clone"** (bouton en haut à droite)
   - Choisir votre workspace
   - Cliquer "Clone"

4. **Voilà !** Vous avez maintenant votre copie du notebook

5. **Sélectionner un cluster** (dropdown en haut) → si pas de cluster, créer un simple Node

6. **Suivre les cellules** en exécutant chacune avec **Run** ou **Shift+Enter**

---

## Partager le notebook avec les participants

### Avant l'atelier

1. Dans votre notebook Lab 2 (une fois créé/importé)
2. Cliquer sur **"Share"** (bouton en haut à droite)
3. Changer les permissions à **"Anyone can view"** ou **"Anyone with link"**
4. Copier le lien
5. **Envoyer le lien par email/Slack** à vos participants

Les participants cliquent, clonent, et c'est bon ! Aucune installation requise.

---

## Structure du Lab 2

| Section | Durée | Contenu |
|---------|-------|---------|
| **1. Concepts** | 5 min | MapReduce pattern, 3 phases (MAP-SHUFFLE-REDUCE) |
| **2. Word Count** | 10 min | Implémentation Spark du "Hello World" du Big Data |
| **3. Cas Finance** | 20 min | Concentration sectorielle (Bâle IV) sur 100k crédits |
| **4. Bonus** | 10 min | Performance : Spark vs boucle Python naïve |
| **Total** | **45 min** | Prêt pour l'atelier en salle |

---

## Dépannage rapide

### ❌ "Error : No compute/cluster available"
**Solution :** créer un cluster (voir "Configurer un cluster" ci-dessus)

### ❌ "ModuleError : pyspark.sql"
**Solution :** vérifier que vous êtes sur Databricks Community (pas Colab ou autre), relancer le cluster

### ❌ "Notebook does not load"
**Solution :** actualiser la page (F5), vider le cache du navigateur

### ✅ "tout fonctionne !" 
Bravo ! Vous êtes prêt à faire l'atelier.

---

## Conseils pédagogiques

**Pendant l'atelier :**

1. **Exécutez chaque section en direct** devant les participants
2. **Arrêtez-vous après chaque cellule** pour expliquer le résultat
3. **Laissez-les modifier le code** : changez le nombre de crédits (1M au lieu de 100k), le seuil Bâle IV, etc.
4. **Posez des questions** : "pourquoi ce secteur est concentré ?", "comment on pourrait diversifier ?"
5. **Connectez à la théorie** : rappeler que c'est du MapReduce, juste plus rapide que Hadoop

---

## Après l'atelier

Les participants gardent leur copie du notebook. Ils peuvent :
- ✅ Le rejouer à la maison
- ✅ Le modifier avec leurs propres données
- ✅ Le partager avec leurs collègues
- ✅ L'utiliser comme reference pour comprendre Spark

---

**Questions ? Demandez au formateur !**

Lab 2 © ISCE Casablanca, Master Finance Digitale 2024-2025
