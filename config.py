import json

# Charger la configuration JSON
with open("config.json", "r") as file:
    CONFIG = json.load(file)
    
    
# Vérifier que toutes les clés impératives sont présentes
REQUIRED_KEYS = ["username", "password", "host", "port", "db_name"]
for key in REQUIRED_KEYS:
    if key not in CONFIG: # python avec in vérifie uniquement les clés
        raise KeyError(f"La clé '{key}' est manquante dans le fichier config.json.")
    

# Exposer les valeurs de configuration sous forme de variables
DB_USERNAME = CONFIG["username"] 
DB_PASSWORD = CONFIG["password"]
DB_HOST = CONFIG["host"]
DB_PORT = CONFIG["port"]
DB_NAME = CONFIG["db_name"]

# Chemin du dossier contenant les fichiers Parquet
DATA_FOLDER = CONFIG.get("data_folder", "data_yellow_taxis")  # Valeur par défaut : data_yellow_taxis