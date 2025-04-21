import pyarrow.parquet as pq
import requests
import logging
import os
import pandas as pd


# configurer le logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


# fonction pour afficher le résumé des téléchargements
def summarize_downloads(
    total: int,
    success: int,
    skipped: int,
    failed: int
) -> None:
    """
    Affiche un résumé des téléchargements effectués.

    Args:
        total (int): Nombre total de fichiers à télécharger.
        success (int): Nombre de fichiers téléchargés avec succès.
        skipped (int): Nombre de fichiers ignorés.
        failed (int): Nombre de fichiers échoués.

    Returns:
        None: Cette fonction ne retourne rien, mais affiche un résumé des téléchargements.

    """
    logging.info(f"Résumé des téléchargements :")
    logging.info(f"  Total : {total}")
    logging.info(f"  Réussis : {success}")
    logging.info(f"  Ignorés : {skipped}")
    logging.info(f"  Échecs : {failed}")
    if success == 0:
        logging.warning("Aucun fichier n'a été téléchargé avec succès.")


# cette fonction va vérifier si le fichier est de type parquet et non vide, sinon il va le supprimer
# on fait la distinction entre un fichier vide et un fichier corrompu ou invalide
def validate_parquet_file(file_name: str) -> None:
    """
    Vérifie si le fichier est de type parquet et non vide, sinon il le supprime.

    Args:
        file_name (str): Le nom du fichier à valider.

    Raises:
        RuntimeError: Si le fichier est vide ou corrompu.

    Returns:
        None: Cette fonction ne retourne rien, mais lève une exception si le fichier est vide ou corrompu.
    """
    try:

        # si le fichier est vide
        if os.path.getsize(file_name) == 0:
            # avec logging on enregistre l'erreur dans le fichier de log
            logging.error(f"Fichier vide : {file_name}")
            # Supprime le fichier vide
            os.remove(file_name)
            # avec raise, on stoppe le programme et on affiche que le fichier est supprimé
            raise RuntimeError(f"Fichier vide : {file_name} => supprimé")

        # si le fichier n'est pas de type parquet, une ecxeption sera levée
        pq.read_table(file_name)
    except Exception as e:
        # avec logging on enregistre l'erreur dans le fichier de log
        logging.error(
            f"Fichier corrompu ou invalide : {file_name}. Erreur : {e}")
        # Supprime le fichier corrompu
        os.remove(file_name)
        # avec raise, on stoppe le programme et on affiche que le fichier est supprimé
        raise RuntimeError(
            f"Fichier corrompu ou invalide : {file_name} => supprimé")


# cette fonction va télécharger des fichier parquet concernat les yellow taxis à partir d'année de début et d'année de fin sur ce site : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
def download_yellow_taxis_files_years(
    annee_debut: int,
    annee_fin: int,
    mois_debut: int = 1,
    mois_fin: int = 12
) -> None:
    """
    Télécharge les fichiers parquet des yellow taxis pour une plage d'années et de mois spécifiée.
    Les fichiers sont enregistrés dans le dossier "data_yellow_taxis".
    Si le fichier existe déjà et est valide, il est ignoré.
    Si le fichier existe mais est invalide, il est remplacé.
    Si le téléchargement échoue, un message d'erreur est affiché.

    Args:
        annee_debut (int): L'année de début pour le téléchargement.
        annee_fin (int): L'année de fin pour le téléchargement.
        mois_debut (int): Le mois de début pour le téléchargement. Par défaut, 1.
        mois_fin (int): Le mois de fin pour le téléchargement. Par défaut, 12.

    Raises:
        ValueError: Si l'année de début est supérieure à l'année de fin.
        ValueError: Si l'année de début est inférieure à 2009.
        ValueError: Si les mois ne sont pas compris entre 1 et 12.
        ValueError: Si les paramètres 'annee_debut' et 'annee_fin' ne sont pas des entiers.
        RuntimeError: Si le téléchargement échoue ou si le fichier est corrompu ou vide.

    Returns:
        None: Cette fonction ne retourne rien, mais télécharge les fichiers parquet et les enregistre dans le dossier "data_yellow_taxis".
    """
    # Crée le dossier s'il n'existe pas
    os.makedirs("data_yellow_taxis", exist_ok=True)

    total, success, skipped, failed = 0, 0, 0, 0

    for year in range(annee_debut, annee_fin+1):
        for month in range(mois_debut, mois_fin+1):

            # on vérifie annee_fin >= annee_debut
            if annee_fin < annee_debut:
                raise ValueError(
                    "L'année de début doit être inférieure ou égale à l'année de fin.")

            # on vérifie que annee_debut >= 2009
            if annee_debut < 2009:
                raise ValueError(
                    "Il n'y a pas de données avant 2009.")

            # on vérifie que les donnés entrées sont bien des int
            if not isinstance(annee_debut, int) or not isinstance(annee_fin, int):
                raise ValueError(
                    "Les paramètres 'annee_debut' et 'annee_fin' doivent être des entiers.")

            # on vérifie si l'année et le mois sont valides
            if not (1 <= mois_debut <= 12) or not (1 <= mois_fin <= 12):
                raise ValueError(
                    "Les mois doivent être compris entre 1 et 12.")
            if annee_debut > annee_fin:
                raise ValueError(
                    "L'année de début doit être inférieure ou égale à l'année de fin.")

            # compteur pour summarize_downloads()
            total += 1

            # le nom du fichier parquet qui va être téléchargé, et rangé dans le dossier data_yellow_taxis
            file_name = f"data_yellow_taxis/yellow_tripdata_{year}-{month:02d}.parquet"

            # vérifie si le fichier existe déjà dans le dossier data_yellow_taxis et s'il est valide, si oui, on l'ignore
            if os.path.exists(file_name):
                try:
                    validate_parquet_file(file_name)
                    logging.info(
                        f"Fichier déjà existant, téléchargement ignoré : {file_name}")

                    # compteur pour summarize_downloads()
                    skipped += 1
                    continue
                except RuntimeError:
                    logging.warning(
                        f"Fichier existant invalide, il sera remplacé : {file_name}")

            # on obtiens tous les lien de téléchargement des fichiers parquet (mois et années)
            download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

            # si le téléchargement ne fonctionne pas, on le stoppe et on affiche un message d'erreur
            try:
                # retourne un objet response qui contient les informations (.status_code) et le contenu(.content) de la requète HTTP
                response = requests.get(download_url)

                # si le code de statut HTTP est 200 (qui correspond au fait que la requète a réussis) et si le contenu n'est pas vide
                # on écrit le contenu du fichier dans le fichier parquet, sinon on affiche un message d'erreur
                if response.status_code == 200 and response.content:
                    # wb = write binary, pour écrire en binaire, essentiel pour les fichiers parquet
                    with open(file_name, "wb") as file:
                        file.write(response.content)
                    # on affiche un message d'information pour dire que le fichier a été téléchargé avec succès
                    logging.info(
                        f"Fichier téléchargé avec succès : {file_name}")

                    # compteur pour summarize_downloads()
                    success += 1

                    # Valide le fichier après téléchargement
                    validate_parquet_file(file_name)
                else:
                    # si le code de statut HTTP n'est pas 200, on affiche un message d'erreur et le téléchargement continue pour les autres fichiers
                    logging.error(
                        f"Impossible de télécharger {file_name}. Code de statut HTTP: {response.status_code}")
                    failed += 1
            # si une exception se produit lors de la requète HTTP, on affiche un message d'erreur
            except requests.exceptions.RequestException as e:
                '''requests.exceptions.RequestException est une classe de base pour toutes les exceptions liées aux requêtes HTTP dans requests. Cela inclut des erreurs comme :
                    Timeout : La requête a pris trop de temps.
                    ConnectionError : Problème de connexion au serveur.
                    HTTPError : Réponse HTTP invalide ou inattendue.
                    TooManyRedirects : Trop de redirections.'''
                raise RuntimeError(
                    f"Erreur lors du téléchargement de {file_name} : {e}")
    summarize_downloads(total, success, skipped, failed)


# lit le fichier parquet et le transforme en dataframe pandas
def transform_files_from_parquet_to_pandas(filepath: str) -> pd.DataFrame:
    """
    Transforme un fichier parquet en DataFrame pandas.

    Args:
        filepath (str): Le chemin du fichier parquet à transformer.

    Returns:
        pd.DataFrame: Le DataFrame pandas résultant.
    """
    try:
        table = pq.read_table(filepath)
        df = table.to_pandas()
        return df
    # en cas d'erreur, on affiche un message d'erreur et on stoppe le programme
    except Exception as e:
        logging.error(
            f"Erreur lors de la transformation du fichier {filepath} en DataFrame : {e}")
        raise
