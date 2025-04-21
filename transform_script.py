import pandas as pd
import numpy as np
import logging
from typing import Callable  # pour faire un type hint de type fonction


# 0) FONCTIONS GENERALES

# configurer le logging pour afficher les messages d'erreur et d'information
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


# cette fonction va vérifier que la colonne existe dans le dataframe, sinon elle lève une erreur
def check_column_exists(df: pd.DataFrame, column_name: str) -> None:
    """
    Vérifie si une colonne existe dans le DataFrame.
    Si la colonne n'existe pas, lève une KeyError.

    Args:
        df (pd.DataFrame): Le DataFrame à vérifier.
        column_name (str): Le nom de la colonne à vérifier.

    Raises:
        KeyError: Si la colonne n'existe pas dans le DataFrame.

    Returns:
        None: Si la colonne existe.
    """
    if column_name not in df.columns:
        raise KeyError(
            f"Erreur : La colonne '{column_name}' n'existe pas dans le DataFrame.")


# 1) NETTOYAGE DES DONNEES

# va effacer les lignes qui sont identiques sur toutes les colonnes
def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les lignes du DataFrame qui sont identiques sur toutes les colonnes.

    Args:
        df (pd.DataFrame): Le DataFrame à nettoyer.

    Returns:
        pd.DataFrame: Le DataFrame sans les lignes dupliquées.
    """
    df = df.drop_duplicates()
    return df


# 2) GESTIONS VALEURS MANQUANTES

# va retourner les lignes qui pour respectent les conditions (les valeurs de la colonne sont dans la liste)
def list_values_respected(
    df: pd.DataFrame,
    column_name: str,
    list_values: list
) -> pd.DataFrame:
    """
    Retourne les lignes du DataFrame où les valeurs de la colonne spécifiée sont dans la liste donnée.

    Args:
        df (pd.DataFrame): Le DataFrame à filtrer.
        column_name (str): Le nom de la colonne à vérifier.
        list_values (list): La liste des valeurs autorisées.

    Returns:
        pd.DataFrame: Le DataFrame filtré.
    """
    df = df[df[column_name].isin(list_values)]
    return df


# cette fonction va supprimer les lignes qui contiennent des valeurs nulles dans les colonnes de la liste entrée
def remove_rows_with_nulls_in_columns(
    df: pd.DataFrame,
    list_column_name: list = [
        'VendorID',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'RatecodeID',
        'store_and_fwd_flag',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge',
        'airport_fee'
    ]
) -> pd.DataFrame:
    """
    Supprime les lignes du DataFrame qui contiennent des valeurs nulles dans les colonnes spécifiées.

    Args:
        df (pd.DataFrame): Le DataFrame à nettoyer.
        list_column_name (list): La liste des noms de colonnes à vérifier.

    Returns:
        pd.DataFrame: Le DataFrame sans les lignes contenant des valeurs nulles dans les colonnes spécifiées.
    """
    df = df.dropna(subset=list_column_name)
    return df


# 3) NORMALISATION DES DONNEES

# cette fonction va supprimer les caractères spéciaux et les espaces dans les colonnes de la liste entrée, et va mettre les valeurs en minuscule
def remove_special_characters(df: pd.DataFrame, list_column_name: list) -> pd.DataFrame:
    """
    Supprime les caractères spéciaux et les espaces dans les colonnes spécifiées du DataFrame,
    et met les valeurs en minuscule.

    Args:
        df (pd.DataFrame): Le DataFrame à nettoyer.
        list_column_name (list): La liste des noms de colonnes à normaliser.
        
    Raises:
        TypeError: Si une colonne ne contient pas de chaînes de caractères.

    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes normalisées.
    """
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        check_column_exists(df, column)

        # Vérifie que la colonne contient des chaînes de caractères
        if not pd.api.types.is_string_dtype(df[column]):
            raise TypeError(
                f"Erreur : La colonne '{column}' ne contient pas de chaînes de caractères.")

        # Supprime les caractères spéciaux, les espaces et met les valeurs en minuscule
        df[column] = df[column].str.strip().str.lower()
    return df


# 4) VALIDATION ET FILTRAGE DES DONNEES

# retourne les lignes qui respectent le format de date
def date_format_respected(
    df: pd.DataFrame,
    list_column_name: list = ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
    date_format: str = "%Y-%m-%d %H:%M:%S"
) -> pd.DataFrame:
    """
    Retourne les lignes du DataFrame où les colonnes spécifiées respectent le format de date donné.

    Args:
        df (pd.DataFrame): Le DataFrame à filtrer.
        list_column_name (list): La liste des noms de colonnes à vérifier.
        date_format (str): Le format de date à respecter. Par défaut : "%Y-%m-%d %H:%M:%S".
        
    Raises:
        TypeError: Si une colonne ne contient pas de valeurs compatibles avec un format de date.
        ValueError: Si une colonne devient entièrement vide après le filtrage.

    Returns:
        pd.DataFrame: Le DataFrame filtré.
    """
    for column in list_column_name:

        # Vérifie que la colonne existe dans le DataFrame
        check_column_exists(df, column)

        # vérifie qu'il existe des valeurs de type datetime dans la colonne, sinon on lève une erreur
        if not pd.api.types.is_datetime64_any_dtype(df[column]):
            raise TypeError(
                f"Erreur : La colonne '{column}' ne contient pas de valeurs compatibles avec un format de date.")

        # Utilise pd.to_datetime avec errors='coerce' pour convertir les valeurs incorrectes en NaT
        # Convertit la colonne en datetime avec le format spécifié
        df[column] = pd.to_datetime(
            df[column], format=date_format, errors="coerce")

    # Filtre les lignes où la colonne n'est pas NaT (c'est-à-dire où le format est respecté)
    df = df.dropna(subset=list_column_name)

    # Vérifie si une colonne est devenue entièrement vide après le filtrage
    for column in list_column_name:
        if df[column].isna().all():
            raise ValueError(
                f"Erreur : Toutes les valeurs de la colonne '{column}' sont invalides après le filtrage.")
    return df


# 5) TRANSFORMATIONS DES DONNEES

# renommer des colonnes (avec un dictionnaire du type {'old_name': 'new_name', 'old_name2': 'new_name2', etc})
def rename_columns(df: pd.DataFrame, columns_dict: dict) -> pd.DataFrame:
    """
    Renomme les colonnes du DataFrame selon le dictionnaire donné.

    Args:
        df (pd.DataFrame): Le DataFrame à modifier.
        columns_dict (dict): Un dictionnaire où les clés sont les noms de colonnes existants et les valeurs sont les nouveaux noms.

    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes renommées.
    """
    for column in columns_dict.keys():

        # Vérifie que la colonne existe dans le DataFrame
        check_column_exists(df, column)

        # Vérifie que la nouvelle colonne n'existe pas déjà
        if columns_dict[column] in df.columns:
            raise KeyError(
                f"Erreur : La colonne '{columns_dict[column]}' existe déjà dans le DataFrame.")

    df = df.rename(columns=columns_dict)
    return df


# transforme miles en km
def miles_to_km(miles: float) -> float:
    """
    Transforme une distance en miles en kilomètres.

    Args:
        miles (float): La distance en miles.

    Returns:
        float: La distance en kilomètres, arrondie à deux décimales.
    """
    return round(miles * 1.609344, 2)


# transforme les miles en km dans la colonne trip_distance
def trip_distance_miles_to_km(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme la distance de la colonne 'trip_distance' du DataFrame de miles en kilomètres.

    Args:
        df (pd.DataFrame): Le DataFrame à modifier.

    Returns:
        pd.DataFrame: Le DataFrame avec la colonne 'trip_distance' convertie en kilomètres.
    """
    df["trip_distance"] = df["trip_distance"].apply(lambda x: miles_to_km(x))
    return df


# convertit les dollars en euros
def dollars_to_euros(dollars: float) -> float:
    """
    Convertit une valeur en dollars en euros.

    Args:
        dollars (float): La valeur en dollars.

    Returns:
        float: La valeur en euros, arrondie à deux décimales.
    """
    # Si la valeur est NaN, on la retourne sans modification
    if pd.isna(dollars):
        return np.NaN
    # Sinon, on applique la conversion
    return round(dollars * 0.92, 2)


# convertit les dollars en autre devise (euros par défaut) dans les colonnes de la liste entrée avec des valeur par défaut
# /!\ à partir de 2025, une colonne sera ajoutée, cbd_congestion_fee, elle n'est pas comprise dans la liste par défaut, il faudra l'ajouter dans la liste si on veut la convertir en euros /!\
def convert_dollars_columns_to_other_devise(
    df: pd.DataFrame,
    other_devise: Callable[[float], float] = dollars_to_euros,
    list_column_name: list = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                              'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
) -> pd.DataFrame:
    """
    Convertit les valeurs des colonnes spécifiées du DataFrame de dollars en une autre devise.
    Par défaut, la conversion est faite en euros.

    Args:
        df (pd.DataFrame): Le DataFrame à modifier.
        other_devise (Callable[[float], float]): La fonction de conversion à appliquer. Par défaut : dollars_to_euros.
        list_column_name (list): La liste des noms de colonnes à convertir.

                Par défaut : ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                            'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'].

    Raises:
        TypeError: Si une colonne ne contient pas de valeurs numériques.
        KeyError: Si une colonne n'existe pas dans le DataFrame.
    
    Returns:
        pd.DataFrame: Le DataFrame avec les colonnes converties.
    """
    for column in list_column_name:

        # Vérifie que la colonne existe dans le DataFrame
        check_column_exists(df, column)

        # Vérifie que la colonne contient des valeurs numériques
        if not pd.api.types.is_numeric_dtype(df[column]):
            raise TypeError(
                f"Erreur : La colonne '{column}' ne contient pas de valeurs numériques.")

        # Applique la fonction sur chaque valeur de la colonne
        df[column] = df[column].apply(other_devise)

    return df


# *) ETAPE FINALE: APPLIQUER LES TRANSFORMATIONS SELECTIONNEES PARMIS LES FONCTIONS DEFINIES CI-DESSUS

# cette fonction va appliquer une liste de transformations sur un dataframe, ces tranformations sont les fonctions définies au dessus
# Exemple d'utilisation : transformations = [(trip_distance_miles_to_km, {}), (date_format_respected, {'column_name': 'pickup_datetime', 'date_format': '%Y-%m-%d %H:%M:%S'})]
def apply_transformations(df_entree: pd.DataFrame, transformations: list) -> pd.DataFrame:
    """
    Applique une liste de transformations sur un DataFrame.
    Chaque transformation est une fonction qui prend le DataFrame en entrée et retourne un DataFrame modifié.

    Args:
        df_entree (pd.DataFrame): Le DataFrame d'entrée.
        transformations (list): Une liste de tuples où chaque tuple contient une fonction de transformation et ses arguments.

            Par exemple => [(fonction1, kwargs1), (fonction2, kwargs2), ...] où kwargs est un dictionnaire contenant les arguments de la fonction.

            Par exemple => (date_format_respected, {'column_name': 'pickup_datetime', 'date_format': '%Y-%m-%d %H:%M:%S'}).

    Raises:
        RuntimeError: Si une transformation échoue en raison d'une valeur invalide.
        KeyError: Si une colonne n'existe pas dans le DataFrame.
        TypeError: Si une colonne ne contient pas de valeurs numériques ou de chaînes de caractères.
    
    Returns:
        pd.DataFrame: Le DataFrame après application de toutes les transformations.
    """
    df = df_entree.copy()
    # transformations est une liste de tuples (fonction, kwargs), kwargs est un dictionnaire qui contient les arguments de la fonction
    for func, kwargs in transformations:
        try:
            # Applique la transformation
            logging.info(f"Application de la transformation : {func.__name__}")
            df = func(df, **kwargs)
        # Gestion des valeurs invalides
        except ValueError as e:
            raise RuntimeError(
                f"Erreur de valeur dans la transformation '{func.__name__}': {e}")
    # On reset l'index après toutes les transformations
    df = df.reset_index(drop=True)
    logging.info("Toutes les transformations ont été appliquées avec succès.")
    return df
