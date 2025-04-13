import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import logging


# 0) FONCTIONS GENERALES

# configurer le logging pour afficher les messages d'erreur et d'information
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


# cette fonction va vérifier que la colonne existe dans le dataframe, sinon elle lève une erreur
def check_column_exists(df, column_name):
    if column_name not in df.columns:
        raise KeyError(
            f"Erreur : La colonne '{column_name}' n'existe pas dans le DataFrame.")


# 1) NETTOYAGE DES DONNEES

# va effacer les lignes qui sont identiques sur toutes les colonnes
def drop_duplicates(df):

    df = df.drop_duplicates()
    return df


# 2) GESTIONS VALEURS MANQUANTES

# va retourner les lignes qui pour respectent les conditions (les valeurs de la colonne sont dans la liste)
def list_values_respected(df, column_name, list_values):

    df = df[df[column_name].isin(list_values)]
    return df


# cette fonction va supprimer les lignes qui contiennent des valeurs nulles dans les colonnes de la liste entrée
def remove_rows_with_nulls_in_columns(df, list_column_name):

    df = df.dropna(subset=list_column_name)
    return df


# 3) NORMALISATION DES DONNEES

# cette fonction va supprimer les caractères spéciaux et les espaces dans les colonnes de la liste entrée, et va mettre les valeurs en minuscule
def remove_special_characters(df, list_column_name):

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
def date_format_respected(df, list_column_name, date_format="%Y-%m-%d %H:%M:%S"):

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
def rename_columns(df, columns_dict):

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
def miles_to_km(miles):

    return round(miles * 1.609344, 2)


# transforme les miles en km dans la colonne trip_distance
def trip_distance_miles_to_km(df):

    df["trip_distance"] = df["trip_distance"].apply(lambda x: miles_to_km(x))
    return df


# convertit les dollars en euros
def dollars_to_euros(dollars):

    # Si la valeur est NaN, on la retourne sans modification
    if pd.isna(dollars):
        return np.NaN
    # Sinon, on applique la conversion
    return round(dollars * 0.92, 2)


# convertit les dollars en euros dans les colonnes de la liste entrée avec des valeur par défaut
# /!\ à partir de 2025, une colonne sera ajoutée, cbd_congestion_fee, elle n'est pas comprise dans la liste par défaut, il faudra l'ajouter dans la liste si on veut la convertir en euros /!\
def convert_dollars_columns_to_other_devise(df, other_devise, list_column_name=['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']):
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
def apply_transformations(df_entree, transformations):
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
