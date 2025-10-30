import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
from math import sqrt
import json

# Lecture de notre dataset
try:
    df = pd.read_csv('dataset.csv')
except FileNotFoundError:
    print("❌ Erreur: Le fichier 'dataset.csv' est introuvable. Assurez-vous d'exécuter JsonToCsv.py d'abord.")
    exit()

print(f"Nombre de lignes prises en compte : {len(df)}")

# On retire la colonne que l'on souhaite prédire.
X = df.drop('prix_ttc_eur', axis=1)
y = df['prix_ttc_eur']

# On split notre dataset (train/test).
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# --- MISE À JOUR DE LA SÉLECTION DES COLONNES ---

# 1. Colonnes Numériques (à normaliser/scaler)
num_cols = ['age_ans', 'kilometrage', 'places', 'portes', 'puissance_fiscale', 'puissance_reelle'] 

# 2. Colonnes Catégorielles (à encoder)
cat_cols = ['marque', 'modele', 'energie', 'boite_de_vitesses', 'couleur', 'type_vehicule', 'provenance', 'premiere_main']

# Nettoyage des NaN/valeurs vides sur les colonnes numériques/catégorielles sélectionnées
# Imputation par la médiane du training set (pour les numériques)
X_train[num_cols] = X_train[num_cols].fillna(X_train[num_cols].median())
X_test[num_cols] = X_test[num_cols].fillna(X_train[num_cols].median()) 

# Imputation par la valeur 'manquant' (pour les catégorielles)
X_train[cat_cols] = X_train[cat_cols].fillna('manquant')
X_test[cat_cols] = X_test[cat_cols].fillna('manquant')


# On utilise un pré-processeur pour normaliser nos données.
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), num_cols),
        ('cat', OneHotEncoder(handle_unknown='ignore'), cat_cols)
    ],
    remainder='drop' 
)

# On utilise XGBoost.
model = Pipeline(steps=[('preprocessor', preprocessor), 
                        ('regressor', XGBRegressor(
                            n_estimators=1000,
                            learning_rate=0.05,
                            max_depth=7,
                            subsample=0.7,
                            colsample_bytree=0.7,
                            random_state=42
                        ))])

# Entraînement
print("\nDébut de l'entraînement du modèle XGBoost...")
model.fit(X_train, y_train)
print("Entraînement terminé.")

y_pred = model.predict(X_test)

# On évalue l'erreur.
mse = mean_squared_error(y_test, y_pred)
print(f"\n--- Évaluation du Modèle ---")
print(f"Erreur quadratique moyenne (MSE): {mse:,.2f}")
print(f"L'écart de prix moyen (RMSE) est : {sqrt(mse):,.2f} €")
print(f"Taille moyenne des prédictions (pour contexte): {y_pred.mean():,.2f} €")
print(f"--------------------------")

# --- PRÉDICTION FINALE AVEC CORRECTION D'IMPUTATION ---
try:
    with open("../to_predict/car_config.json", 'r') as fichier_json:
        car_config = json.load(fichier_json)
        
    car_df = pd.DataFrame(car_config, index=[0])
    
    # 1. Imputation Robuste: Gérer les colonnes manquantes dans car_config.json
    for col in X_train.columns:
        if col not in car_df.columns:
            # Correction: Remplacer par la médiane d'entraînement si c'est numérique
            if col in num_cols:
                 # La médiane est un float, ce qui résout l'erreur de conversion.
                 car_df[col] = X_train[col].median()
            # Utiliser 'manquant' si c'est une colonne catégorielle
            elif col in cat_cols:
                 car_df[col] = 'manquant'
            else:
                 car_df[col] = '' # Pour les autres colonnes ignorées
        
        # 2. Assurer le format numérique (si la valeur existe mais est NaN ou None)
        elif col in num_cols and pd.isna(car_df.loc[0, col]):
            car_df.loc[0, col] = X_train[col].median()
            
        # 3. Uniformiser les chaînes (minuscules)
        elif col in cat_cols:
            car_df[col] = car_df[col].astype(str).str.lower().str.strip()


    car_df = car_df[X_train.columns] # Réordonner les colonnes
    
    prix_predit = model.predict(car_df)
    prix_predit = int(prix_predit[0])

    print(f"Le prix prédit pour la {car_config.get('marque', 'Véhicule Inconnu')} {car_config.get('modele', '')} est de : {prix_predit:,}€")

except FileNotFoundError:
    print("\n⚠️ Fichier car_config.json manquant ou mal situé (attendu dans le répertoire parent). Impossible d'effectuer la prédiction finale.")
except Exception as e:
    # Affiche l'erreur si elle n'est pas due à un fichier manquant
    print(f"\n❌ Erreur lors de la prédiction du car_config: {e}")