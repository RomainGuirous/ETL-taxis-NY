"""
Ce fichier contient la définition de la fonction `run_etl` pour le processus ETL.
Il n'est pas conçu pour être exécuté directement. Importez `run_etl` dans un autre script pour l'utiliser.
"""

import logging
from extract_script import download_yellow_taxis_files_years, transform_files_from_parquet_to_pandas
import transform_script as ts
from load_script import main_load_script
import os
from config import DATA_FOLDER


# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_process.log"),
        logging.StreamHandler()
    ]
)


def run_etl(annee_debut: int,
            annee_fin: int,
            mois_debut: int = 1,
            mois_fin: int = 12,
            table_name: str = "yellow_taxis",
            list_transformations: list = None
            ) -> None:
    """
    Exécute le processus ETL (Extraction, Transformation, Chargement) pour les données de taxis jaunes de New York.

    Args:
            annee_debut (int): L'année de début pour le téléchargement.
            annee_fin (int): L'année de fin pour le téléchargement.
            mois_debut (int): Le mois de début pour le téléchargement. Par défaut, 1.
            mois_fin (int): Le mois de fin pour le téléchargement. Par défaut, 12.
            table_name (str): Nom de la table cible dans la base de données. Par défaut "yellow_taxis".
            list_transformations (list): Liste des transformations à appliquer aux données. Par défaut cette liste est: 
                [
                    (ts.drop_duplicates, {"df": "df"}),
                    (ts.list_values_respected, {
                        "df": "df",
                        "column_name": "VendorID",
                        "list_values": [1, 2, 6, 7]
                    }),
                    (ts.list_values_respected, {
                        "df": "df",
                        "column_name": "RatecodeId",
                        "list_values": [1, 2, 3, 4, 5, 6, 99]
                    }),
                    (ts.list_values_respected, {
                        "df": "df",
                        "column_name": "store_and_fwd_flag",
                        "list_values": ["Y", "N"]
                    }),
                    (ts.list_values_respected, {
                        "df": "df",
                        "column_name": "payment_type",
                        "list_values": [0, 1, 2, 3, 4, 5, 6]
                    }),
                    (ts.remove_rows_with_nulls_in_columns, {
                        "df": "df"
                    }),
                    (ts.date_format_respected, {
                        "df": "df"
                    }),
                ].

    Raises:


    Returns:
        None: Cette fonction ne retourne rien, mais exécute le processus ETL.
    """
    # les objets mutables (comme les listes) partagés entre plusieurs appels de fonction peuvent causer des comportements imprévisibles.
    # utiliser None comme valeur par défaut et en initialiser la liste à l'intérieur de la fonction évite ce problème
    if list_transformations is None:
        list_transformations: list = [
            (ts.drop_duplicates, {"df": "df"}),
            (ts.list_values_respected, {
                "df": "df",
                "column_name": "VendorID",
                "list_values": [1, 2, 6, 7]
            }),
            (ts.list_values_respected, {
                "df": "df",
                "column_name": "RatecodeId",
                "list_values": [1, 2, 3, 4, 5, 6, 99]
            }),
            (ts.list_values_respected, {
                "df": "df",
                "column_name": "store_and_fwd_flag",
                "list_values": ["Y", "N"]
            }),
            (ts.list_values_respected, {
                "df": "df",
                "column_name": "payment_type",
                "list_values": [0, 1, 2, 3, 4, 5, 6]
            }),
            (ts.remove_rows_with_nulls_in_columns, {
                "df": "df"  # list_column_name non précisé pour appliquer valeurs par défaut
            }),
            (ts.date_format_respected, {
                "df": "df"  # list_column_name et format non précisés pour appliquer valeurs par défaut
            }),
        ]
    try:
        # Étape 1 : Extraction
        logging.info("Début de l'étape d'extraction.")
        download_yellow_taxis_files_years(
            annee_debut,
            annee_fin,
            mois_debut,
            mois_fin
        )
        logging.info("Étape d'extraction terminée avec succès.")

        # Étape 2 : Transformation
        logging.info("Début de l'étape de transformation.")

        # Vérifier si le dossier configuré dans config.py existe, sinon on stoppe le programme
        if not os.path.exists(DATA_FOLDER):
            logging.error(f"Le dossier {DATA_FOLDER} n'existe pas.")
            raise FileNotFoundError(f"Le dossier {DATA_FOLDER} n'existe pas.")
        else:
            # os.listdir("data_yellow_taxis") retourne une liste de tous les fichiers dans le dossier data_yellow_taxis
            for file_name in os.listdir("data_yellow_taxis"):

                # Vérifier si le fichier est de type Parquet
                if file_name.endswith(".parquet"):

                    # on crée le chemin complet du fichier
                    file_path = os.path.join(DATA_FOLDER, file_name)
                    logging.info(f"Transformation du fichier : {file_path}")

                    # on transforme le fichier parquet en dataframe pandas
                    df = transform_files_from_parquet_to_pandas(file_path)

                    # on applique les transformations sur le dataframe pandas
                    transformed_data = ts.apply_transformations(
                        df, list_transformations)
                    logging.info(
                        f"Transformation terminée pour le fichier : {file_path}")

                    # Étape 3 : Chargement
                    logging.info("Début de l'étape de chargement.")
                    main_load_script(table_name, transformed_data)
                    logging.info("Étape de chargement terminée avec succès.")

    except Exception as e:
        logging.error(f"Une erreur s'est produite lors du processus ETL : {e}")
        raise
