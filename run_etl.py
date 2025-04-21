import logging
from extract_script import download_yellow_taxis_files_years, transform_files_from_parquet_to_pandas
from transform_script import 
from load_script import load_data_to_database
import pandas as pd


# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_process.log"),
        logging.StreamHandler()
    ]
)


def run_etl():
    try:
        # Étape 1 : Extraction
        logging.info("Début de l'étape d'extraction.")
        download_yellow_taxis_files_years(annee_debut=2020, annee_fin=2021)
        logging.info("Étape d'extraction terminée avec succès.")

        # Étape 2 : Transformation
        logging.info("Début de l'étape de transformation.")
        transformed_data = transform_files_from_parquet_to_pandas(
                       "data_yellow_taxis/yellow_tripdata_2020-01.parquet")
        logging.info("Étape de transformation terminée avec succès.")

        # Étape 3 : Chargement
        logging.info("Début de l'étape de chargement.")
        load_data_to_database(transformed_data)
        logging.info("Étape de chargement terminée avec succès.")

    except Exception as e:
        logging.error(f"Une erreur s'est produite lors du processus ETL : {e}")
        raise


if __name__ == "__main__":
    logging.info("Démarrage du processus ETL.")
    run_etl()
    logging.info("Processus ETL terminé.")
