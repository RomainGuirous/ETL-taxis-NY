from sqlalchemy import create_engine, text

username = ''
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'db_name'

conn = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}")

# Vérification des paramètres de connexion
def validate_connection_params(username, password, host, port):
    if not username or not password or not host or not port:
        raise ValueError("Les paramètres de connexion (username, password, host, port) doivent être renseignés.")


# Création de la base de données
def create_database(conn, db_name):
    with conn.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        print(f"Base de données '{db_name}' vérifiée/créée.")


# Création d'une table dans la base de données
def create_table(conn, table_name):
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
        print(f"Table '{table_name}' vérifiée/créée.")
        
        
def insert_data(conn, table_name, df):
    # Insertion des données dans la table
    with conn.connect() as connection:
        #si la table existe déjà, on la remplace,index=False pour ne pas ajouter une colonne index
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        print(f"Données insérées dans la table '{table_name}'.")
        
        
def main():
    validate_connection_params(username, password, host, port)
    create_database(conn, DB_NAME)
    create_table(conn, 'your_table')