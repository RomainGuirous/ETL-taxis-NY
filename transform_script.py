import pyarrow.parquet as pq
import pandas as pd
import os

pd.set_option('display.max_columns', None)

# def transform_files_from_parquet_to_pandas(filepath):
#     #lire le fichier parquet
#     table = pq.read_table(filepath)
#     #transformer le fichier parquet en dataframe
#     df = table.to_pandas()
#     return df

#transforme miles en km
def miles_to_km(miles):
    return miles * 1.609344

#transforme les miles en km dans la colonne trip_distance
def trip_distance_miles_to_km(df_entree):
    df = df_entree.copy()
    df['trip_distance'] = df['trip_distance'].apply(lambda x: miles_to_km(x))
    return df

#va retourner les lignes qui pour respectent les conditions (les valeurs de la colonne sont dans la liste)
def list_values_respected(df_entree, column_name, list_values):
    df = df_entree.copy()
    df = df[df[column_name].isin(list_values)]
    
#va effacer les lignes qui sont identiques sur toutes les colonnes
def drop_duplicates(df_entree):
    df = df_entree.copy()
    df = df.drop_duplicates()
    return df

#retourne les lignes qui respectent le format de date
def date_format_respected(df_entree, column_name, date_format='%Y-%m-%d %H:%M:%S'):
    df = df_entree.copy()
    # Utilise pd.to_datetime avec errors='coerce' pour convertir les valeurs incorrectes en NaT
    df[column_name] = pd.to_datetime(df[column_name], format=date_format, errors='coerce')
    # Filtre les lignes où la colonne n'est pas NaT (c'est-à-dire où le format est respecté)
    df = df[df[column_name].notna()]
    return df

#renommer des colonnes (avec un dictionnaire du type {'old_name': 'new_name', 'old_name2': 'new_name2', etc})
def rename_columns(df_entree, columns_dict):
    df = df_entree.copy()
    df = df.rename(columns=columns_dict)
    return df


#cette fonction va appliquer une liste de transformations sur un dataframe, ces tranformations sont les fonctions définies au dessus
#Exemple d'utilisation : transformations = [(trip_distance_miles_to_km, {}), (date_format_respected, {'column_name': 'pickup_datetime', 'date_format': '%Y-%m-%d %H:%M:%S'})]
def apply_transformations(df_entree, transformations): #transformations est une liste de tuples (fonction, kwargs)
    df_transformed = df_entree.copy()
    for func, kwargs in transformations:
        df_transformed = func(df_transformed, **kwargs)
    # On reset l'index après toutes les transformations
    df = df.reset_index(drop=True)
    return df_transformed