import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os

pd.set_option("display.max_columns", None)


def transform_files_from_parquet_to_pandas(filepath):
    # lire le fichier parquet
    table = pq.read_table(filepath)
    # transformer le fichier parquet en dataframe
    df = table.to_pandas()
    return df


# 1) NETTOYAGE DES DONNEES

# va effacer les lignes qui sont identiques sur toutes les colonnes

def drop_duplicates(df_entree):
    df = df_entree.copy()
    df = df.drop_duplicates()
    return df


# 2) GESTIONS VALEURS MANQUANTES

# va retourner les lignes qui pour respectent les conditions (les valeurs de la colonne sont dans la liste)

def list_values_respected(df_entree, column_name, list_values):
    df = df_entree.copy()
    df = df[df[column_name].isin(list_values)]


# cette fonction va supprimer les lignes qui contiennent des valeurs nulles dans les colonnes de la liste entrée

def remove_rows_with_nulls_in_columns(df_entree, list_column_name):
    df = df_entree.copy()
    df = df.dropna(subset=list_column_name)
    return df


#     df = df_entree.copy()
#   # Vérifie que toutes les colonnes spécifiées existent dans le DataFrame
#     missing_columns = [col for col in list_column_name if col not in df.columns]
#     if missing_columns:
#         print(f"Les colonnes suivantes sont manquantes : {missing_columns}")
#         # Vous pouvez lever une exception si ces colonnes sont critiques
#         return df

#     # Supprime les lignes contenant des valeurs nulles dans les colonnes spécifiées
#     df = df.dropna(subset=list_column_name)
#     return df

# 3) NORMALISATION DES DONNEES

# cette fonction va supprimer les caractères spéciaux et les espaces dans les colonnes de la liste entrée, et va mettre les valeurs en minuscule


def remove_special_characters(df_entree, list_column_name):
    df = df_entree.copy()
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        if column in df.columns:
            # supprime les caractères spéciaux et les espaces et met les valeurs en minuscule
            try:
                df[column] = df[column].str.strip().str.lower()
            except Exception as e:
                print(f"Column : {column}  does not exist in the DataFrame.")
    return df


# 4) VALIDATION ET FILTRAGE DES DONNEES

# retourne les lignes qui respectent le format de date

def date_format_respected(df_entree, list_column_name, date_format="%Y-%m-%d %H:%M:%S"):
    df = df_entree.copy()
    # Utilise pd.to_datetime avec errors='coerce' pour convertir les valeurs incorrectes en NaT
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        if column in df.columns:
            # Convertit la colonne en datetime avec le format spécifié
            try:
                df[column] = pd.to_datetime(
                    df[column], format=date_format, errors="coerce")
            except Exception as e:
                print(f"Column : {column}  does not exist in the DataFrame.")
    # Filtre les lignes où la colonne n'est pas NaT (c'est-à-dire où le format est respecté)
    df = df.dropna(subset=list_column_name)
    return df

# 5) TRANSFORMATIONS DES DONNEES

# renommer des colonnes (avec un dictionnaire du type {'old_name': 'new_name', 'old_name2': 'new_name2', etc})


def rename_columns(df_entree, columns_dict):
    df = df_entree.copy()
    df = df.rename(columns=columns_dict)
    return df


# transforme miles en km

def miles_to_km(miles):
    return round(miles * 1.609344, 2)


# transforme les miles en km dans la colonne trip_distance

def trip_distance_miles_to_km(df_entree):
    df = df_entree.copy()
    df["trip_distance"] = df["trip_distance"].apply(lambda x: miles_to_km(x))
    return df


# convertit les dollars en euros

def dollars_to_euros(dollars):
    if pd.isna(dollars):  # Vérifie si la valeur est NaN
        return np.NaN  # Retourne NaN sans modification
    return round(dollars * 0.92, 2)


# convertit les dollars en euros dans les colonnes de la liste entrée avec des valeur par défaut

def convert_dollars_columns_to_other_devise(df_entree, other_devise, list_column_name=['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']):
    df = df_entree.copy()
    for column in list_column_name:
        # Vérifie que la colonne existe dans le DataFrame
        if column in df.columns:
            # Applique la fonction sur chaque valeur de la colonne
            try:
                df[column] = df[column].apply(other_devise)
            except Exception as e:
                print(f"Column : {column}  does not exist in the DataFrame.")
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
        # Gestion des colonnes manquantes
        except KeyError as e:
            print(
                f"Erreur : Colonne manquante dans la fonction '{func.__name__}'. Détails : {e}")
        # Gestion des valeurs invalides
        except ValueError as e:
            print(
                f"Erreur : Valeur invalide dans la fonction '{func.__name__}'. Détails : {e}")
        # Gestion des erreurs inattendues
        except Exception as e:
            print(f"Erreur inattendue dans la fonction '{func.__name__}': {e}")
    # On reset l'index après toutes les transformations
    df = df.reset_index(drop=True)
    return df


df = transform_files_from_parquet_to_pandas(
    "data_yellow_taxis/yellow_tripdata_2019-01.parquet")
print('début')
print(apply_transformations(
    df, [
        (drop_duplicates, {
        }),
        (remove_rows_with_nulls_in_columns, {
            'list_column_name': ['passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID']
        }),
        (date_format_respected, {
            "list_column_name": ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        }),
        (convert_dollars_columns_to_other_devise, {
            'other_devise': dollars_to_euros,
            'list_column_name': ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
        })
    ]).head())
# print(df.columns.values)
# print(df.head())
print('fin')
