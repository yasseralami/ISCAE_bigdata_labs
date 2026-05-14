# Guide d'importation Lab 2 dans Databricks Free Edition
## Instructions détaillées pas-à-pas

---

## Étape 0 : Préparer votre fichier Lab 2

Vous devez avoir le fichier **`Lab2_Hadoop_Spark_Finance.py`** téléchargé sur votre ordinateur.

---

## Étape 1 : Se connecter à Databricks Free Edition

1. Aller sur **https://www.databricks.com/try-databricks**
2. Cliquer **"Free"** (pas Pro ni Premium)
3. Créer votre compte (email + password)
4. Vérifier votre email
5. Vous êtes dans votre **Workspace** Databricks

**Vous devez voir une page ressemblant à :**
```
📊 Databricks
├─ Workspace (menu gauche)
├─ Compute
├─ Repos
└─ Admin Settings
```

---

## Étape 2 : Créer un dossier pour vos Labs (optionnel mais recommandé)

1. Dans le menu gauche, cliquer sur **"Workspace"**
2. Cliquer sur votre **email** (en haut de la liste)
3. Cliquer sur **"Create"** → **"Folder"**
4. Nommer le dossier : **`Labs`**
5. Cliquer **"Create"**

Résultat : vous avez maintenant un dossier `/Users/votre-email/Labs`

---

## Étape 3 : Importer le fichier Lab 2

### Méthode A : Importer depuis un fichier Python (RECOMMANDÉ)

1. Cliquer sur **"Workspace"** (menu gauche)
2. Naviguer dans le dossier **`Labs`** (si créé) ou rester à la racine
3. Cliquer sur **"Import"** (bouton en haut ou menu)
4. Choisir **"File"**
5. Cliquer sur **"Choose file"** → sélectionner **`Lab2_Hadoop_Spark_Finance.py`**
6. Cliquer **"Import"**

**⏱️ Patience** : Databricks convertit le fichier Python en notebook (30 secondes)

✅ **Résultat** : un notebook nommé `Lab2_Hadoop_Spark_Finance` apparaît dans votre Workspace

---

### Méthode B : Copier-coller le code (alternative)

Si l'import ne fonctionne pas :

1. Ouvrir le fichier `Lab2_Hadoop_Spark_Finance.py` dans un éditeur texte
2. Copier tout le contenu (Ctrl+A → Ctrl+C)
3. Dans Databricks, cliquer **"Create"** → **"Notebook"**
4. Nommer : `Lab 2 - Hadoop, MapReduce & Spark Finance`
5. Langage : **Python**
6. Cliquer **"Create"**
7. Coller le code dans le notebook (Ctrl+V)
8. Cliquer **"Save"** (Ctrl+S)

---

## Étape 4 : Créer un cluster

**Important** : un notebook Databricks a besoin d'un cluster pour exécuter le code.

### Créer votre premier cluster

1. **Menu gauche** → **"Compute"**
2. Cliquer sur **"Create compute"**
3. Remplir les infos :
   - **Cluster name** : `Lab-Cluster` (ou ce que vous voulez)
   - **Cluster type** : Single Node (gratuit, suffisant pour la démo)
   - **Databricks Runtime** : dernière version (ex : 14.3 LTS)
   - **Node type** : laissez par défaut (i3.xlarge)
   - **Auto-terminate** : 30 minutes (économise les crédits)
   
4. Cliquer **"Create compute"**
5. **Attendre le lancement** (~2-3 minutes, barre verte "RUNNING")

---

## Étape 5 : Attacher le cluster au notebook Lab 2

1. Ouvrir le notebook **`Lab2_Hadoop_Spark_Finance`**
2. En haut du notebook, voir un dropdown **"No cluster"** ou **"Detached"**
3. Cliquer sur ce dropdown
4. Choisir le cluster créé : **`Lab-Cluster`**
5. Cliquer pour attacher

**✅ Résultat** : le notebook est maintenant connecté au cluster (barre verte)

---

## Étape 6 : Tester que tout fonctionne

1. Dans le notebook, cliquer sur la **première cellule de code**
   (celle qui commence par `# Données : liste de mots`)

2. Cliquer le bouton **"Run"** (▶️) ou appuyer **Shift+Enter**

3. **Résultats attendus** :
   ```
   ✅ Données en mémoire distribuée (RDD)
   Nombre total de mots : 11
   Premiers 5 mots : ['hadoop', 'mapreduce', 'spark', 'hadoop', 'spark']
   ```

**Si ça s'affiche :** ✅ Bravo ! Tout fonctionne.

**Si erreur** : vérifier que le cluster est bien attaché et en état "RUNNING"

---

## Étape 7 : Partager le notebook avec les participants (optionnel)

Si vous voulez que les participants utilisent votre notebook plutôt que de cloner :

1. En haut du notebook, cliquer sur **"Share"** (bouton en haut à droite)
2. Choisir les permissions :
   - **"Anyone with link can view"** (lecture seule)
   - **"Anyone with link can edit"** (modifiable)
3. Copier le lien
4. Envoyer le lien aux participants par email/Slack

**Les participants :**
1. Cliquent sur le lien
2. Cliquent **"Clone"** pour avoir leur propre copie
3. Sélectionnent le cluster
4. Exécutent les cellules

---

## ✅ Checklist avant l'atelier

- [ ] Databricks Free Edition créé
- [ ] Dossier `/Labs` créé (optionnel)
- [ ] Fichier Lab 2 importé
- [ ] Cluster créé et lancé
- [ ] Cluster attaché au notebook
- [ ] Première cellule exécutée avec succès
- [ ] Lien partagé envoyé aux participants (optionnel)

---

## 🆘 Dépannage rapide

### ❌ "No cluster selected"
**Solution :** Menu haut → dropdown cluster → choisir `Lab-Cluster`

### ❌ "Cluster is not running"
**Solution :** Menu gauche → Compute → cliquer sur le cluster → bouton "Start"

### ❌ "Import failed / fichier non reconnu"
**Solution :** utiliser la Méthode B (créer notebook + copier-coller)

### ❌ "Aucun résultat après exécution"
**Solution :** vérifier que le cluster est bien lancé (barre verte en haut du notebook)

### ✅ "tout fonctionne !"
Excellent ! Vous êtes prêt pour l'atelier.

---

## 📺 Raccourcis utiles dans Databricks

| Action | Raccourci |
|--------|-----------|
| Exécuter une cellule | **Shift+Enter** |
| Créer une nouvelle cellule | **Ctrl+Alt+N** |
| Supprimer une cellule | **Ctrl+Shift+X** |
| Sauvegarder le notebook | **Ctrl+S** |
| Rechercher/remplacer | **Ctrl+H** |
| Afficher l'aide | **Ctrl+/** |

---

## 📝 Notes supplémentaires

**Structure du Lab 2 :**
```
Section 1 : Concepts MapReduce (5 min)
Section 2 : Word Count (10 min)
Section 3 : Cas finance Bâle IV (20 min)
Section 4 : Bonus Performance (10 min)
```

**Durée totale : 45 minutes**

Vous pouvez :
- Exécuter le notebook complet en direct
- Faire des pauses pour expliquer chaque section
- Laisser les participants modifier le code (ex: changer le nombre de crédits)

---

**Des questions ? N'hésitez pas à demander au support Databricks (?) ou à revoir ce guide.**

Lab 2 © ISCAE Casablanca, Master Finance Digitale 2025-2026
