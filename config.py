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
# .get() renvoie None si la clé n'existe pas, évitant ainsi une KeyError, et/ou permet de définir une valeur par défaut
DB_USERNAME = CONFIG["username"] 
DB_PASSWORD = CONFIG["password"]
DB_HOST = CONFIG.get("host", "localhost")  # Valeur par défaut : localhost
DB_PORT = CONFIG.get("port", 3306)         # Valeur par défaut : 3306
DB_NAME = CONFIG.get("db_name", "ny_taxi_db")  # Valeur par défaut : ny_taxi_db