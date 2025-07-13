from sqlalchemy import create_engine, text, Engine
import logging
from config import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
import pandas as pd


# Configuration du logging
# Logging permet de remplacer les print par des logs, ce qui est plus propre et permet de mieux gérer les erreurs
# level=logging.INFO permet de ne pas afficher les messages de niveau DEBUG, et de de garder des messages plus concis
# format permet de personnaliser le format des messages de log (ici on affiche la date, le niveau de log et le message)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# création de la connexion à la base de données
def get_connection(
    username: str, password: str, host: str, port: int, db_name: str = None
) -> Engine:
    """
    Crée une connexion à la base de données MySQL.
    Si db_name est None, la connexion se fera au serveur MySQL renseigné dans config.
    Si db_name est spécifié, la connexion sera à cette base de données.

    Args:
        username (str): Nom d'utilisateur de la base de données.
        password (str): Mot de passe de la base de données.
        host (str): Adresse du serveur de base de données.
        port (int): Port du serveur de base de données.
        db_name (str, optional): Nom de la base de données. Par défaut None.

    Returns:
        engine: Un objet de connexion à la base de données.
    """
    if db_name:
        db_uri = f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}"
    else:
        db_uri = f"mysql+pymysql://{username}:{password}@{host}:{port}"

    logging.info(f"Connexion à la base de données : {db_uri}")
    return create_engine(db_uri)


# Vérification des paramètres de connexion
def validate_connection_params(
    username: str, password: str, host: str, port: int, db_name: str
) -> None:
    """
    Vérifie les paramètres de connexion à la base de données.

    Args:
        username (str): Nom d'utilisateur de la base de données.
        password (str): Mot de passe de la base de données.
        host (str): Adresse du serveur de base de données.
        port (int): Port du serveur de base de données.

    Raises:
        ValueError: Si les paramètres de connexion sont invalides.

    Returns:
        None: Cette fonction ne retourne rien, mais lève une exception si les paramètres de connexion sont invalides.
    """
    if not username or not password or not host or not port or not db_name:
        raise ValueError(
            "Les paramètres de connexion (username, password, host, port, db_name) doivent être renseignés."
        )


# Création de la base de données
def create_database(conn: Engine, db_name: str) -> None:
    """
    Crée une base de données si elle n'existe pas déjà.

    Args:
        conn: Connexion à la base de données.
        db_name (str): Nom de la base de données à créer.

    Returns:
        None: Cette fonction ne retourne rien, mais crée la base de données si elle n'existe pas.
    """
    try:
        with conn.connect() as connection:
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logging.info(f"Base de données '{db_name}' vérifiée/créée.")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la base de données : {e}")


# Vérification de l'existence de la table dans la base de données
def table_exists(conn: Engine, table_name: str) -> bool:
    """
    Vérifie si une table existe dans la base de données.

    Args:
        conn: Connexion à la base de données.
        table_name (str): Nom de la table à vérifier.

    Returns:
        bool: True si la table existe, False sinon.
    """
    # information_schema.tables est une table système qui contient des informations sur toutes les tables de la base de données
    query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    with conn.connect() as connection:
        # donne un objet de type CursorResult, scalar() renvoie le premier élément de la première ligne du résultat de la requête, qui correspond à la requete SQL
        result = connection.execute(text(query)).scalar()
        # booléen
        return result > 0


# Création d'une table dans la base de données
def create_table(conn: Engine, table_name: str) -> None:
    """
    Crée une table dans la base de données si elle n'existe pas déjà.

    Args:
        conn: Connexion à la base de données.
        table_name (str): Nom de la table à créer.

    Returns:
        None: Cette fonction ne retourne rien, mais crée la table si elle n'existe pas.
    """
    try:
        with conn.connect() as connection:
            # Création de la table
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vendorid INT,
                tpep_pickup_datetime DATETIME,
                tpep_dropoff_datetime DATETIME,
                passenger_count INT,
                trip_distance FLOAT,
                ratecodeid INT,
                store_and_fwd_flag VARCHAR(1),
                PULocationID INT,
                DOLocationID INT,
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
        raise Exception(f"Erreur lors de la création de la table : {e}")


# Insertion des données dans la table
def insert_data(
    conn: Engine, table_name: str, df: pd.DataFrame, mode: str = "development"
) -> None:
    """
    Insère des données dans une table de la base de données.

    Args:
        conn: Connexion à la base de données.
        table_name (str): Nom de la table dans laquelle insérer les données.
        df (pd.DataFrame): DataFrame contenant les données à insérer.

    Returns:
        None: Cette fonction ne retourne rien, mais insère les données dans la table.
    """
    try:
        with conn.connect() as connection:
            if mode == "development":
                # En mode développement : vider la table mais garder la structure (avec id AUTO_INCREMENT)
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                df.to_sql(table_name, con=connection, if_exists="append", index=False)
                logging.info(
                    f"Données remplacées dans la table '{table_name}' (mode: {mode})."
                )
            else:
                df.to_sql(table_name, con=connection, if_exists="append", index=False)
                logging.info(f"Données insérées dans la table '{table_name}'.")
    except Exception as e:
        logging.error(f"Erreur lors de l'insertion des données : {e}")


# Fonction principale pour orchestrer la création de la base de données et de la table
def main_load_script(
    table_name: str, df: pd.DataFrame, mode: str = "development"
) -> None:
    """
    Orchestre le processus de création de la base de données, de la table et d'insertion des données.
    Crée connexion au serveur MySQL, crée la base de données si elle n'existe pas.
    Se connecte à la base de données, vérifie l'existence de la table et la crée si nécessaire, puis insère les données dans la table.

    Args:
        table_name (str): Nom de la table à créer.
        df (pd.DataFrame): DataFrame contenant les données à insérer.
        mode (str): Mode d'insertion ("development" ou "production"). Par défaut "development".

    Returns:
        None: Cette fonction ne retourne rien, mais orchestre l'ensemble du processus.
    """
    try:
        validate_connection_params(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

        # Connexion au serveur MySQL
        server_conn = get_connection(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT)
        create_database(server_conn, DB_NAME)

        # Connexion à la base de données
        conn = get_connection(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

        # Gestion de la création de table selon le mode
        if mode == "development":
            # En mode développement : toujours recréer la table avec la bonne structure
            with conn.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            create_table(conn, table_name)
        else:
            # En mode production : créer la table seulement si elle n'existe pas
            if not table_exists(conn, table_name):
                create_table(conn, table_name)

        insert_data(conn, table_name, df, mode)
    except Exception as e:
        logging.error(f"Erreur dans la fonction principale : {e}")
