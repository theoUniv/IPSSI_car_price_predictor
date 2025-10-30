# 🚗 **Car Price Predictor** 🚗

Ce projet a pour but de créer un pipeline complet pour la prédiction du prix des voitures d'occasion. Il se compose de trois étapes principales :

Collecte de Données (Scraping) : Utilisation de Scrapy et Playwright pour extraire les fiches techniques des véhicules depuis le site Autosphere.

Préparation des Données (ETL) : Nettoyage, normalisation, et ingénierie de variables (comme le calcul de l'âge du véhicule) pour structurer les données dans un fichier dataset.csv.

Modélisation (Machine Learning) : Entraînement d'un modèle de régression XGBoost pour prédire le prix des voitures en fonction de leurs caractéristiques.

Prérequis

Assurez-vous d'avoir Python 3.8+ et les outils suivants installés.

Installation des dépendances Python

Ce projet nécessite plusieurs bibliothèques, notamment pour le scraping (Scrapy, Playwright) et le Machine Learning (Pandas, Scikit-learn, XGBoost).

pour les installer : 
` pip install scrapy scrapy-playwright pandas scikit-learn xgboost `

# Installez les navigateurs nécessaires pour Playwright
` playwright install `


🛠️ Utilisation

Le pipeline complet se déroule en trois étapes :

Étape 1 : Collecte des Données (Scraping)

Le spider spider_corrigé.py parcourt les pages de recherche et collecte les données de chaque fiche véhicule en utilisant Playwright pour gérer les chargements JavaScript.

Lancer le Spider :
Exécutez cette commande à la racine de votre projet. Les fichiers JSON seront sauvegardés dans le dossier scrapped/.

` scrapy runspider spider_corrigé.py `


Note: Le spider est configuré pour collecter un maximum de 100 pages par défaut (MAX_PAGES). Vous pouvez ajuster cette valeur dans le fichier spider_corrigé.py si nécessaire.

Étape 2 : Préparation du Dataset (JSON vers CSV)

Une fois les données brutes collectées en JSON, le script de traitement nettoie les valeurs, calcule des variables importantes (comme l'âge en années) et consolide tout en un fichier dataset.csv prêt pour l'entraînement.

Exécuter le script ETL :

` python3 JsonToCsv.py `
# Ceci va lire les fichiers dans 'scrapped/' et générer 'dataset.csv'


Étape 3 : Entraînement du Modèle et Prédiction

Le script d'entraînement utilise le dataset.csv pour former le modèle XGBoost, évalue sa performance (RMSE), et effectue une prédiction sur un exemple de configuration de voiture.

Créer un fichier de configuration pour la prédiction :
Créez un dossier to_predict/ et un fichier car_config.json à la racine pour tester votre modèle.

Exemple de to_predict/car_config.json:

{
    "marque": "PEUGEOT",
    "modele": "208",
    "energie": "essence",
    "boite_de_vitesses": "manuelle",
    "couleur": "gris",
    "type_vehicule": "citadine",
    "provenance": "FRANCE",
    "premiere_main": "Non",
    "kilometrage": 50000,
    "puissance_fiscale": 5,
    "puissance_reelle": 100,
    "portes": 5,
    "places": 5,
    "age_ans": 3
}


Lancer l'entraînement et la prédiction :
Ce script est basé sur la logique vue dans votre fichier train_model.py.

python3 train_model.py
# Le script affichera le RMSE et le prix prédit pour la voiture dans car_config.json.


Structure du Projet

.
├── scrapped/                 # Dossier où les fichiers JSON bruts sont stockés
│   ├── fiche_0.json
│   ├── fiche_1.json
│   └── ...
├── to_predict/               # Dossier pour la configuration de prédiction
│   └── car_config.json       # Fichier pour tester la prédiction finale
├── dataset.csv               # Dataset nettoyé, prêt pour l'entraînement (produit par JsonToCsv.py)
├── spider_corrigé.py         # Spider Scrapy/Playwright (Collecte)
├── JsonToCsv.py              # Script de nettoyage et d'agrégation (ETL)
└── train_model.py            # Script d'entraînement XGBoost (Modélisation)
