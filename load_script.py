from sqlalchemy import create_engine, text
import logging
from config import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


# Configuration du logging
# Logging permet de remplacer les print par des logs, ce qui est plus propre et permet de mieux gérer les erreurs
# level=logging.INFO permet de ne pas afficher les messages de niveau DEBUG, et de de garder des messages plus concis
# format permet de personnaliser le format des messages de log (ici on affiche la date, le niveau de log et le message)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


# création de la connexion à la base de données
def get_connection(username, password, host, port, db_name=None):
    db_uri = f"mysql+pymysql://{username}:{password}@{host}:{port}"
    if db_name:
        db_uri += f"/{db_name}"
    logging.info(f"Connexion à la base de données : {db_uri}")
    return create_engine(db_uri)


# Vérification des paramètres de connexion
def validate_connection_params(username, password, host, port):
    if not username or not password or not host or not port:
        raise ValueError(
            "Les paramètres de connexion (username, password, host, port) doivent être renseignés.")


# Création de la base de données
def create_database(conn, db_name):
    try:
        with conn.connect() as connection:
            connection.execute(
                text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logging.info(f"Base de données '{db_name}' vérifiée/créée.")
    except Exception as e:
        logging.error(
            f"Erreur lors de la création de la base de données : {e}")


# Vérification de l'existence de la base de données
def table_exists(conn, table_name):
    # information_schema.tables est une table système qui contient des informations sur toutes les tables de la base de données
    query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    with conn.connect() as connection:
        # donne un objet de type CursorResult, scalar() renvoie le premier élément de la première ligne du résultat de la requête, qui correspond à la requete SQL
        result = connection.execute(text(query)).scalar()
        # booléen
        return result > 0


# Création d'une table dans la base de données
def create_table(conn, table_name):
    try:
        with conn.connect() as connection:
            # Création de la table
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vendorId INT,
                tpep_pickup_datetime DATETIME,
                tpep_dropoff_datetime DATETIME,
                passenger_count INT,
                trip_distance FLOAT,
                rate_code_id INT,
                store_and_fwd_flag VARCHAR(1),
                pickup_location_id INT,
                dropoff_location_id INT,
                payment_type INT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT,
                cbd_congestion_fee FLOAT DEFAULT NULL
            )
            """
            connection.execute(text(create_table_query))
            logging.info(f"Table '{table_name}' vérifiée/créée.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table : {e}")


# Insertion des données dans la table
def insert_data(conn, table_name, df):
    try:
        with conn.connect() as connection:
            # si la table existe déjà, on la remplace,index=False pour ne pas ajouter une colonne index
            df.to_sql(table_name, con=connection,
                      if_exists='replace', index=False)
            logging.info(f"Données insérées dans la table '{table_name}'.")
    except Exception as e:
        logging.error(f"Erreur lors de l'insertion des données : {e}")


# Fonction principale pour orchestrer la création de la base de données et de la table
def main(table_name, df):
    try:
        conn = get_connection(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
        validate_connection_params(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT)
        create_database(conn, DB_NAME)
        if not table_exists(conn, table_name):
            create_table(conn, table_name)
        insert_data(conn, table_name, df)
    except Exception as e:
        logging.error(f"Erreur dans la fonction principale : {e}")
