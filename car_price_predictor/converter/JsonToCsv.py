import pandas as pd
import json
import os
import re
from datetime import datetime

json_dir = "scrapped/"
outputCsv = "database/dataset.csv"

# --- 1. SÉLECTION ET NETTOYAGE DES CHAMPS ---
def clean_and_normalize_data(json_file_path):
    """Charge un fichier JSON, sélectionne les champs pertinents et les nettoie."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Erreur lors du chargement de {json_file_path}: {e}")
        return None

    # Sélection des champs non redondants et pertinents
    cleaned_data = []
    for item in data:
        # Si le prix est manquant, on ignore la ligne
        if 'prix_ttc_eur' not in item:
            continue

        new_item = {
            # Cible (Y)
            'prix_ttc_eur': item.get('prix_ttc_eur'),
            
            # Caractéristiques principales (X)
            'nom_complet_vehicule': item.get('nom_complet_vehicule', ''),
            'energie': item.get('menu_energie', ''),
            'boite_de_vitesses': item.get('menu_boite_de_vitesses', ''),
            'couleur': item.get('menu_couleur', ''),
            'type_vehicule': item.get('menu_categorie', ''), # Ex: SUV, Berline
            'provenance': item.get('menu_provenance', ''),
            'premiere_main': item.get('menu_premiere_main', ''),

            # Champs Numériques à nettoyer
            'kilometrage': item.get('menu_kilometrage', ''),
            'date_mise_en_circulation': item.get('menu_date_de_mise_en_circulation', ''),
            'puissance_fiscale': item.get('menu_puissance_fiscale', ''),
            'puissance_reelle': item.get('menu_puissance_reelle', ''),
            'portes': item.get('menu_portes', ''),
            'places': item.get('menu_places', ''),
            
            # Champs à considérer (à vous de voir si vous les gardez ou non)
            'longueur': item.get('menu_longueur', ''),
            'largeur': item.get('menu_largeur', ''),
            'hauteur': item.get('menu_hauteur', ''),
            'poids': item.get('menu_poids', ''),
            'volume_coffre': item.get('menu_volume_du_coffre', ''),
            'air_quality_icon': item.get('menu_air_quality_icon', ''),
            'ville': item.get('menu_ville', ''),
            
            # Champs ignorés: url, reference, et les doublons (acheter_, entretenir_, etc.)
        }
        cleaned_data.append(new_item)
    
    if not cleaned_data:
        return None
        
    df = pd.DataFrame(cleaned_data)
    
    # --- 2. FONCTIONS DE NETTOYAGE ET CONVERSION ---
    
    # Nettoyage des chaînes numériques (garde uniquement les chiffres et points, puis convertit en numérique)
    def clean_numeric_string(s):
        if pd.isna(s) or s == '':
            return None
        # Supprime tout sauf les chiffres, le point, et remplace la virgule par le point
        s = str(s).replace(',', '.').replace('\u202f', '').replace('\xa0', '').replace(' ', '')
        s = re.sub(r'[^\d.]', '', s)
        try:
            return float(s)
        except ValueError:
            return None

    # Extraction Marque et Modèle du nom complet
    def extract_brand_model(name):
        parts = str(name).split(' ')
        # Logique simplifiée: La marque est souvent le premier mot
        marque = parts[0] if parts else ''
        # Le modèle est souvent le deuxième mot
        modele = parts[1] if len(parts) > 1 else ''
        return marque, modele

    # Calcul de l'âge du véhicule (Feature Engineering)
    def calculate_car_age(date_str):
        if pd.isna(date_str) or date_str == '':
            return None
        try:
            # Assurez-vous que le format de la date (JJ/MM/AAAA) est correct
            date_immat = datetime.strptime(date_str, '%d/%m/%Y')
            # Utilisez la date d'aujourd'hui pour calculer l'âge
            age_jours = (datetime.now() - date_immat).days
            # On retourne l'âge en jours ou en années (les jours sont plus précis)
            return age_jours / 365.25
        except ValueError:
            return None

    # --- 3. APPLICATION DES TRANSFORMATIONS ---
    
    # Appliquer le nettoyage aux colonnes numériques/quantitatives
    df['kilometrage'] = df['kilometrage'].apply(clean_numeric_string)
    df['puissance_reelle'] = df['puissance_reelle'].apply(clean_numeric_string)
    df['portes'] = df['portes'].apply(clean_numeric_string).fillna(5).astype(int) # 5 est une valeur par défaut raisonnable si manquant
    df['places'] = df['places'].apply(clean_numeric_string).fillna(5).astype(int) # 5 est une valeur par défaut raisonnable si manquant
    df['puissance_fiscale'] = df['puissance_fiscale'].apply(clean_numeric_string)
    
# Nettoyage des champs de taille (souvent moins critiques)
    df['longueur'] = df['longueur'].apply(clean_numeric_string)
    df['largeur'] = df['largeur'].apply(clean_numeric_string)    # <-- LIGNE AJOUTÉE
    df['hauteur'] = df['hauteur'].apply(clean_numeric_string)     # <-- LIGNE AJOUTÉE
    df['poids'] = df['poids'].apply(clean_numeric_string)
    df['volume_coffre'] = df['volume_coffre'].apply(clean_numeric_string)

    # Feature Engineering (Age du véhicule)
    df['age_ans'] = df['date_mise_en_circulation'].apply(calculate_car_age)
    df.drop(columns=['date_mise_en_circulation'], inplace=True)

    # Extraction Marque/Modèle
    df[['marque', 'modele']] = df['nom_complet_vehicule'].apply(lambda x: pd.Series(extract_brand_model(x)))
    df.drop(columns=['nom_complet_vehicule'], inplace=True)
    
    # Conversion de toutes les chaînes restantes en minuscules pour l'uniformité
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.lower().str.strip().replace('nan', '') # Remplacer 'nan' textuel par vide

    # --- 4. GESTION DES VALEURS MANQUANTES ET NETTOYAGE FINAL ---

    # Remplacer les NaN numériques (après nettoyage) par la médiane/moyenne ou 0 si c'est pour l'encodage
    # Ici on fait un choix simple: on va laisser les NaN pour le moment, ils seront gérés par le préprocesseur scikit-learn ou on les supprimera si critiques.
    
    # Suppression des lignes où le kilométrage, l'âge ou la puissance sont manquants (critiques pour le prix)
    df.dropna(subset=['kilometrage', 'age_ans', 'puissance_reelle', 'puissance_fiscale'], inplace=True)
    
    return df

# --- 5. LOGIQUE PRINCIPALE ---
all_data = []
json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]

print(f"Début du traitement de {len(json_files)} fichiers JSON...")

for i, json_file in enumerate(json_files):
    if(i<10000000): # Maintien de votre limite
        df_cleaned = clean_and_normalize_data(os.path.join(json_dir, json_file))
        if df_cleaned is not None:
            all_data.append(df_cleaned)
        print(f"✅ Traité {i+1}/{len(json_files)}: {json_file}. {len(df_cleaned) if df_cleaned is not None else 0} lignes conservées.")
    else: 
        pass

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Suppression des lignes avec prix manquant ou égal à zéro (non entraînable)
    final_df = final_df[final_df['prix_ttc_eur'] > 0] 
    
    # Sauvegarde finale
    final_df.to_csv(outputCsv, index=False, encoding='utf-8')
    print(f"\n✨ FIN DU TRAITEMENT. {len(final_df)} lignes sauvegardées dans {outputCsv} avec succès.")
else:
    print("\n❌ Aucun fichier JSON valide trouvé ou aucune donnée n'a été conservée après nettoyage.")