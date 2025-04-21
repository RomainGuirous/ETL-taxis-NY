# ETL-taxis-NY

Ce projet implémente une pipeline ETL (Extract, Transform, Load) pour traiter les données des taxis jaunes de New York disponibles sur le site officiel :https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.

L'objectif est d'automatiser le téléchargement des données, leur transformation, et leur chargement dans une base de données MySQL.

---

## **Introduction**

Ce projet vise à automatiser le traitement des données des taxis jaunes de New York. Ces données, disponibles publiquement, contiennent des informations sur les trajets, les paiements, et bien plus encore. La pipeline ETL permet de :
- Télécharger automatiquement les fichiers de données.
- Nettoyer et transformer les données pour les rendre exploitables.
- Charger les données dans une base de données MySQL pour des analyses ultérieures.

---

## **Technologies utilisées**

- **Python** :
  - `Pandas` : Manipulation et transformation des données.
  - `Pyarrow` : Lecture des fichiers Parquet.
  - `Requests` : Téléchargement des fichiers depuis le site web.
  - `SQLAlchemy` : Connexion et interaction avec la base de données MySQL.
- **MySQL** : Base de données pour stocker les données transformées.

---

## **Structure du projet**

Voici les principaux fichiers et leur rôle :

- **`config.py`** : Contient les configurations globales (chemins, informations de connexion à la base de données, etc.).
- **`extract_script.py`** : Script pour télécharger les fichiers de données depuis le site web.
- **`transform_script.py`** : Script pour transformer et nettoyer les données.
- **`load_script.py`** : Script pour charger les données transformées dans la base de données MySQL.
- **`run_etl.py`** : Contient la fonction principale `run_etl()` qui orchestre le processus ETL.
- **`main.py`** : Point d'entrée pour exécuter le processus ETL.
- **`README.md`** : Documentation du projet.
- **`requirements.txt`** : Liste des dépendances Python nécessaires.

---

## **Prérequis**

1. **Python 3.8 ou supérieur** :
   - Assurez-vous que Python est installé sur votre machine. Vous pouvez vérifier la version avec :
     ```bash
     python --version
     ```

2. **MySQL** :
   - Une instance MySQL doit être configurée et accessible.

3. **Dépendances Python** :
   - Installez les bibliothèques nécessaires avec :
     ```bash
     pip install -r requirements.txt
     ```

---

## **Configuration**

1. **Renommer le fichier `config.json.exemple` :**
   - À la racine du projet, vous trouverez un fichier nommé `config.json.exemple`.
   - Renommez ce fichier en `config.json` :
     ```bash
     mv config.json.exemple config.json
     ```

2. **Modifier les paramètres dans `config.json` :**
   - Ouvrez le fichier `config.json` et remplacez les valeurs par vos propres informations :
     ```json
     {
         "username": "votre_utilisateur",
         "password": "votre_mot_de_passe",
         "host": "localhost",
         "port": 3306,
         "db_name": "ny_taxi_db",
         "data_folder": "data_yellow_taxis"
     }
     ```

   - **Description des champs :**
     - `username` : Nom d'utilisateur pour la base de données MySQL.
     - `password` : Mot de passe pour la base de données MySQL.
     - `host` : Adresse du serveur MySQL (par défaut : `localhost`).
     - `port` : Port du serveur MySQL (par défaut : `3306`).
     - `db_name` : Nom de la base de données où les données seront chargées.
     - `data_folder` : Chemin du dossier où les fichiers Parquet seront stockés.

3. **Vérifier la configuration :**
    - Assurez-vous que les informations dans `config.json` sont correctes avant d'exécuter le projet.
---

## **Utilisation**

### **1. Exécution du processus ETL**
Le fichier `main.py` est le point d'entrée pour exécuter le processus ETL. Voici un exemple d'exécution depuis la ligne de commande :

```bash
python main.py --annee_debut 2020 --annee_fin 2021 --mois_debut 1 --mois_fin 12
```

## **Améliorations futures**

- Ajouter la prise en charge des données des taxis verts et des VTC.
- Implémenter des tests unitaires pour chaque étape du processus ETL.
- Ajouter une interface utilisateur pour configurer et exécuter le processus ETL.
- Permettre le chargement des données dans d'autres bases de données (PostgreSQL, SQLite, etc.).