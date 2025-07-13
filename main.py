import argparse
from run_etl import run_etl

if __name__ == "__main__":
    #exemple commande à rentrer en terminal:
    # python main.py --annee_debut 2020 --annee_fin 2020 --mois_debut 1 --mois_fin 1 --table_name yellow_taxis
    
    # Définir les arguments en ligne de commande
    parser = argparse.ArgumentParser(description="Exécuter le processus ETL pour les taxis jaunes de New York.")
    parser.add_argument("--annee_debut", type=int, required=True, help="Année de début pour le téléchargement.")
    parser.add_argument("--annee_fin", type=int, required=True, help="Année de fin pour le téléchargement.")
    parser.add_argument("--mois_debut", type=int, default=1, help="Mois de début (par défaut : 1).")
    parser.add_argument("--mois_fin", type=int, default=12, help="Mois de fin (par défaut : 12).")
    parser.add_argument("--table_name", type=str, default="yellow_taxis", help="Nom de la table cible (par défaut : yellow_taxis).")

    # Lire les arguments
    args = parser.parse_args()

    # Validation des arguments
    if args.annee_debut > args.annee_fin:
        raise ValueError("L'année de début ne peut pas être supérieure à l'année de fin.")
    if args.mois_debut > args.mois_fin:
        raise ValueError("Le mois de début ne peut pas être supérieur au mois de fin.")

    # Exécuter le processus ETL avec les arguments fournis
    run_etl(
        annee_debut=args.annee_debut,
        annee_fin=args.annee_fin,
        mois_debut=args.mois_debut,
        mois_fin=args.mois_fin,
        table_name=args.table_name
    )