import pyarrow.parquet as pq
import pandas as pd
import numpy as np

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
        if column not in df.columns:
            # arrête la fonction (raise) et indique que la colonne n'existe pas (KeyError)
            raise KeyError(
                f"Erreur : La colonne '{column}' n'existe pas dans le DataFrame.")

        try:
            # Supprime les caractères spéciaux, les espaces et met les valeurs en minuscule
            df[column] = df[column].str.strip().str.lower()
        except Exception as e:
            # arrete la fonction (raise) et indique l'erreur (e) qui s'est produite sur quelle colonne (column)
            raise Exception(
                f"Erreur inattendue lors du traitement de la colonne '{column}': {e}")
    return df


# 4) VALIDATION ET FILTRAGE DES DONNEES

# retourne les lignes qui respectent le format de date

def date_format_respected(df, list_column_name, date_format="%Y-%m-%d %H:%M:%S"):
    # Utilise pd.to_datetime avec errors='coerce' pour convertir les valeurs incorrectes en NaT
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        if column not in df.columns:
            raise KeyError(
                f"Erreur : La colonne '{column}' n'existe pas dans le DataFrame.")
        try:
            # Convertit la colonne en datetime avec le format spécifié
            df[column] = pd.to_datetime(
                df[column], format=date_format, errors="coerce")
        except Exception as e:
            raise Exception(
                f"Erreur inattendue lors du traitement de la colonne '{column}': {e}")
    # Filtre les lignes où la colonne n'est pas NaT (c'est-à-dire où le format est respecté)
    df = df.dropna(subset=list_column_name)
    
     # Vérifie si une colonne est devenue entièrement vide après le filtrage
    for column in list_column_name:
        if df[column].isna().all():
            raise ValueError(f"Erreur : Toutes les valeurs de la colonne '{column}' sont invalides après le filtrage.")
    return df


# 5) TRANSFORMATIONS DES DONNEES

# renommer des colonnes (avec un dictionnaire du type {'old_name': 'new_name', 'old_name2': 'new_name2', etc})


def rename_columns(df, columns_dict):
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
    if pd.isna(dollars):  # Vérifie si la valeur est NaN
        return np.NaN  # Retourne NaN sans modification
    return round(dollars * 0.92, 2)


# convertit les dollars en euros dans les colonnes de la liste entrée avec des valeur par défaut

def convert_dollars_columns_to_other_devise(df, other_devise, list_column_name=['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']):
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        if column not in df.columns:
            raise KeyError(
                f"Erreur : La colonne '{column}' n'existe pas dans le DataFrame.")
        # Applique la fonction sur chaque valeur de la colonne
        try:
            df[column] = df[column].apply(other_devise)
        except Exception as e:
            raise Exception(f"Erreur inattendue lors du traitement de la colonne '{column}': {e}")
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
            df = func(df, **kwargs)
        # Gestion des valeurs invalides
        except ValueError as e:
            raise RuntimeError(f"Erreur de valeur dans la transformation '{func.__name__}': {e}")
        # Gestion des erreurs inattendues
        except Exception as e:
            # Ajoute un contexte à l'erreur et la relance
            raise RuntimeError(f"Erreur dans la transformation '{func.__name__}': {e}")
    # On reset l'index après toutes les transformations
    df = df.reset_index(drop=True)
    return df