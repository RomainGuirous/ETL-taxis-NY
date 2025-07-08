# Étape 1 : image de base
#on choisit une image de base légère pour Python 3.12, la dernière version stable
# si besoin, repasser à version 3.11 ou 3.10
# on utilise l'image "slim" pour une version plus légère de Python
# on peut aussi utiliser python:3.12-alpine pour une image encore plus légère
FROM python:3.12-slim

# Étape 2 : définir le dossier de travail dans l'image
WORKDIR /app

# Étape 3 : copier les fichiers de ton projet dans l'image
COPY . /app

# Étape 4 : installer les dépendances
# --no-cache-dir permet de ne pas conserver les fichiers temporaires de pip, ce qui réduit la taille de l'image
RUN pip install --no-cache-dir -r requirements.txt

# Étape 5 : lancer le script principal
# on rentre la commande pour exécuter le script Python principal
# chaque mot ou argument est séparé est un élément de la liste
# ici on est exécute en fait : python main.py
CMD ["python", "main.py"]

# Étape 6 : construire l'image
# pour construire l'image, on utilise la commande suivante dans le terminal (racine du projet) :
# docker build -t etl-taxi-ny -f docker/Dockerfile .

# docker build est la commande pour construire une image Docker
# -t etl-taxi-ny permet de nommer l'image "etl-taxi-ny"
# -f docker/Dockerfile permet de spécifier le chemin du Dockerfile
# (on pourrait nommer ce fichier différemment, mais il est courant de l'appeler Dockerfile)
# -f veut dire "file" (fichier), donc il faut toujours spécifier le chemin d'un fichier
# la dernière partie indique le contexte de construction, c'est-à-dire le répertoire dans lequel Docker va chercher les fichiers à inclure dans l'image
# le point (.) indique que le contexte de construction est le répertoire courant
# si à la place de point (.), on met un autre chemin, Docker cherchera les fichiers à partir de ce chemin

# Pour forcer une reconstruction complète sans cache :
# docker build --no-cache -t etl-taxi-ny -f docker/Dockerfile
# le cache permet de réutiliser les couches précédentes pour accélérer la construction, mais peut parfois causer des problèmes si les dépendances changent


# Étape 7 : sauvegarder l'image
# Pour sauvegarder cette image localement en fichier .tar :
# docker save etl-taxi-ny -o docker/etl-taxi-ny.tar

# docker save est la commande pour sauvegarder une image Docker
# etl-taxi-ny est le nom de l'image à sauvegarder
# -o docker/etl-taxi-ny.tar permet de spécifier le nom du fichier de sortie (si vous ne spécifiez pas le chemin, il sera sauvegardé dans le répertoire courant)
# Sans -o (--output), Docker afficherait l’image sous forme binaire dans la console (inutile).

# Étape 8 : exécuter l'image
# docker run etl-taxi-ny

# docker run est la commande pour exécuter une image Docker
# etl-taxi-ny est le nom de l'image que tu as construite

# Étape 9 : monter en volume config.json
# docker run --rm -v $(pwd)/config.json:/app/config.json etl-taxi-ny
# docker run --rm lance l'image Docker (et supprime le conteneur après son exécution)
# -v <chemin_dossier_ou_fichier_local>:<chemin_dossier_ou_fichier_dans_le_container> permet de monter le fichier config.json du répertoire courant dans le conteneur Docker
# par exemple :  C:/Users/Romain/Documents/ETL-taxis-NY/config.json:/app/config.json
# etl-taxi-ny est le nom de l'image que tu as construite

# Étape BONUS : pour effacer l'image locale :
# docker rmi etl-taxi-ny

# docker rmi est la commande pour supprimer une image Docker
# etl-taxi-ny est le nom de l'image à supprimer ,on peut aussi utiliser l'ID de l'image (IMAGE ID)
