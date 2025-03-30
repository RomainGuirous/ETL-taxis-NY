import pyarrow.parquet as pq
import requests

# cette fonction va télécharger des fichier parquet concernat les yellow taxis à partir d'année de début et d'année de fin sur ce site : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
def download_yellow_taxis_files_years(annee_debut, annee_fin):
    for year in range(annee_debut, annee_fin+1):
        for month in range(1,13):
            # on obtiens tous les lien de téléchargement des fichiers parquet (mois et années)
            download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            # le nom du fichier parquet qui va être téléchargé, et rangé dans le dossier data_yellow_taxis
            file_name = f"data_yellow_taxis/yellow_tripdata_{year}-{month:02d}.parquet"
            
            # retourne un objet response qui contient les informations (.status_code) et le contenu(.content) de la requète HTTP
            response = requests.get(download_url)

            # si le code de statut HTTP est 200 (qui correspond au fait que la requète a réussis), on écrit le contenu du fichier dans le fichier parquet, sinon on affiche un message d'erreur
            if response.status_code == 200:
                with open(file_name, "wb") as file: #wb = write binary, pour écrire en binaire, essentiel pour les fichiers parquet
                    file.write(response.content)
            else:
                print(f"Impossible de télécharger {file_name}. Code de statut HTTP: {response.status_code}")


# lit le fichier parquet et le transforme en dataframe pandas
def transform_files_from_parquet_to_pandas(filepath):
    table = pq.read_table(filepath)
    df = table.to_pandas()
    return df              