import json
import mysql.connector
import re
import datetime
from mysql.connector import Error

# --- CONFIGURATION DE LA BASE DE DONNÉES ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '', 
    'database': 'projet_scraping_cars'
}

JSON_FILE = 'autosphere_data.json'

# --- CORRECTION 1: REGEX NON GOURMANDE ---
URL_REGEX = re.compile(r'/auto-occasion-([a-z0-9-]+?)-([a-z0-9-]+?)')

def parse_url_for_brand_model(url):
    """Extrait la marque et le modèle depuis l'URL."""
    if not url:
        return None, None
    match = URL_REGEX.search(url)
    if match:
        brand = match.group(1).replace('-', ' ').title()
        model = match.group(2).replace('-', ' ').title()
        return brand, model
    return None, None

def calculer_age(date_str):
    """Calcule l'âge à partir d'une date en string (ex: "27/03/2019")."""
    if not date_str:
        return None
    try:
        date_mise_en_circ = datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
        # On utilise la date de fin du projet
        today = datetime.date(2025, 10, 31) 
        
        age = today.year - date_mise_en_circ.year - ((today.month, today.day) < (date_mise_en_circ.month, date_mise_en_circ.day))
        return max(0, age)
    except ValueError:
        return None

def nettoyer_valeur_numerique(valeur_str):
    """Nettoie une chaîne de caractères pour en extraire un entier."""
    if valeur_str is None:
        return None
    chiffres = re.findall(r'\d+', str(valeur_str))
    if not chiffres:
        return None
    try:
        return int("".join(chiffres))
    except ValueError:
        return None

def convertir_premiere_main(valeur_str):
    """Convertit 'Oui'/'Non' en booléen."""
    if valeur_str and 'oui' in valeur_str.lower():
        return True
    return False

def get_ou_creer_id(cursor, table_name, colonne_nom, valeur):
    """Récupère ou crée l'ID pour une table de dimension."""
    if not valeur:
        return None
        
    try:
        query = f"SELECT id FROM {table_name} WHERE {colonne_nom} = %s"
        cursor.execute(query, (valeur,))
        resultat = cursor.fetchone()
        
        if resultat:
            return resultat[0]
        
        insert_query = f"INSERT INTO {table_name} ({colonne_nom}) VALUES (%s)"
        cursor.execute(insert_query, (valeur,))
        return cursor.lastrowid
        
    except Error as e:
        print(f"Erreur get_ou_creer_id ({table_name}): {e}")
        raise e

def get_ou_creer_modele(cursor, id_marque, nom_modele):
    """Fonction spécifique pour les modèles, qui dépendent d'une marque."""
    if not nom_modele or not id_marque:
        return None
    
    try:
        query = "SELECT id FROM Modele WHERE nom_modele = %s AND id_marque = %s"
        cursor.execute(query, (nom_modele, id_marque))
        resultat = cursor.fetchone()
        
        if resultat:
            return resultat[0]
        
        insert_query = "INSERT INTO Modele (nom_modele, id_marque) VALUES (%s, %s)"
        cursor.execute(insert_query, (nom_modele, id_marque))
        return cursor.lastrowid
        
    except Error as e:
        print(f"Erreur get_ou_creer_modele ({nom_modele}): {e}")
        raise e


def creer_schema_normalise(cursor):
    """Crée l'ensemble des tables normalisées pour stocker les données."""
    print("Vérification/Création du schéma de base de données...")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Marque (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nom_marque VARCHAR(255) NOT NULL UNIQUE
    ) ENGINE=InnoDB;
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Modele (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nom_modele VARCHAR(255) NOT NULL,
        id_marque INT NOT NULL,
        FOREIGN KEY (id_marque) REFERENCES Marque(id),
        UNIQUE KEY uk_marque_modele (id_marque, nom_modele)
    ) ENGINE=InnoDB;
    """)

    cursor.execute("CREATE TABLE IF NOT EXISTS Energie (id INT AUTO_INCREMENT PRIMARY KEY, nom_energie VARCHAR(50) NOT NULL UNIQUE) ENGINE=InnoDB;")
    cursor.execute("CREATE TABLE IF NOT EXISTS BoiteDeVitesses (id INT AUTO_INCREMENT PRIMARY KEY, nom_boite VARCHAR(50) NOT NULL UNIQUE) ENGINE=InnoDB;")
    cursor.execute("CREATE TABLE IF NOT EXISTS Couleur (id INT AUTO_INCREMENT PRIMARY KEY, nom_couleur VARCHAR(50) NOT NULL UNIQUE) ENGINE=InnoDB;")
    cursor.execute("CREATE TABLE IF NOT EXISTS Provenance (id INT AUTO_INCREMENT PRIMARY KEY, nom_provenance VARCHAR(50) NOT NULL UNIQUE) ENGINE=InnoDB;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Vehicule (
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(512) NOT NULL UNIQUE,
        nom_complet VARCHAR(255),
        prix_tt_eur INT,
        
        age_ans INT,
        kilometrage INT,
        places INT,
        portes INT,
        puissance_fiscale INT,
        puissance_reelle INT,
        premiere_main BOOLEAN,
        
        id_modele INT,
        id_energie INT,
        id_boite INT,
        id_couleur INT,
        id_provenance INT,
        
        FOREIGN KEY (id_modele) REFERENCES Modele(id),
        FOREIGN KEY (id_energie) REFERENCES Energie(id),
        FOREIGN KEY (id_boite) REFERENCES BoiteDeVitesses(id),
        FOREIGN KEY (id_couleur) REFERENCES Couleur(id),
        FOREIGN KEY (id_provenance) REFERENCES Provenance(id)
    ) ENGINE=InnoDB;
    """)
    print("Schéma prêt.")

def get_field(voiture, key_prefix):
    """
    Tente de récupérer une valeur avec un préfixe (ex: 'bonnes_affaires_').
    Si elle est vide, essaie les autres préfixes.
    """
    val = voiture.get(f"{key_prefix}_kilometrage")
    if val: return val
    
    val = voiture.get(f"menu_{key_prefix}")
    if val: return val
    
    val = voiture.get(f"acheter_{key_prefix}")
    if val: return val
    
    return None

def integrer_donnees(conn):
    """
    Lit le fichier JSON et insère les données dans la BDD MySQL.
    """
    print(f"Chargement des données depuis {JSON_FILE}...")
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ ERREUR: Le fichier '{JSON_FILE}' est introuvable.")
        return
    except json.JSONDecodeError as e:
        print(f"❌ ERREUR: Le fichier '{JSON_FILE}' est mal formé ou vide. Erreur: {e}")
        return

    cursor = conn.cursor()
    creer_schema_normalise(cursor)
    
    print(f"Début de l'intégration de {len(data)} véhicules...")
    
    count_inserted = 0
    count_updated = 0
    count_errors = 0

    for voiture in data:
        try:
            url_vehicule = voiture.get('url')
            nom_marque, nom_modele = parse_url_for_brand_model(url_vehicule)
            
            id_marque = get_ou_creer_id(cursor, 'Marque', 'nom_marque', nom_marque)
            id_modele = get_ou_creer_modele(cursor, id_marque, nom_modele)

            energie = voiture.get('bonnes_affaires_energie') or voiture.get('menu_energie')
            boite = voiture.get('bonnes_affaires_boite_de_vitesses') or voiture.get('menu_boite_de_vitesses')
            couleur = voiture.get('bonnes_affaires_couleur') or voiture.get('menu_couleur')
            provenance = voiture.get('bonnes_affaires_provenance') or voiture.get('menu_provenance')
            
            id_energie = get_ou_creer_id(cursor, 'Energie', 'nom_energie', energie)
            id_boite = get_ou_creer_id(cursor, 'BoiteDeVitesses', 'nom_boite', boite)
            id_couleur = get_ou_creer_id(cursor, 'Couleur', 'nom_couleur', couleur)
            id_provenance = get_ou_creer_id(cursor, 'Provenance', 'nom_provenance', provenance)

            date_circ = voiture.get('bonnes_affaires_date_de_mise_en_circulation') or voiture.get('menu_date_de_mise_en_circulation')
            age_ans = calculer_age(date_circ)
            
            kilo = voiture.get('bonnes_affaires_kilometrage') or voiture.get('menu_kilometrage')
            kilometrage = nettoyer_valeur_numerique(kilo)
            
            pla = voiture.get('bonnes_affaires_places') or voiture.get('menu_places')
            places = nettoyer_valeur_numerique(pla)
            
            por = voiture.get('bonnes_affaires_portes') or voiture.get('menu_portes')
            portes = nettoyer_valeur_numerique(por)
            
            pfisc = voiture.get('bonnes_affaires_puissance_fiscale') or voiture.get('menu_puissance_fiscale')
            puissance_fiscale = nettoyer_valeur_numerique(pfisc)
            
            preelle = voiture.get('bonnes_affaires_puissance_reelle') or voiture.get('menu_puissance_reelle')
            puissance_reelle = nettoyer_valeur_numerique(preelle)
            
            pm = voiture.get('bonnes_affaires_premiere_main') or voiture.get('menu_premiere_main')
            premiere_main = convertir_premiere_main(pm)

            sql_insert = """
                INSERT INTO Vehicule (
                    url, nom_complet, prix_ttc_eur, age_ans, kilometrage, places, portes, 
                    puissance_fiscale, puissance_reelle, premiere_main, 
                    id_modele, id_energie, id_boite, id_couleur, id_provenance
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    prix_ttc_eur = VALUES(prix_ttc_eur),
                    kilometrage = VALUES(kilometrage),
                    age_ans = VALUES(age_ans)
            """
            
            valeurs = (
                url_vehicule,
                voiture.get('nom_complet_vehicule'),
                voiture.get('prix_ttc_eur'),
                age_ans,
                kilometrage,
                places,
                portes,
                puissance_fiscale,
                puissance_reelle,
                premiere_main,
                id_modele,
                id_energie,
                id_boite,
                id_couleur,
                id_provenance
            )
            
            cursor.execute(sql_insert, valeurs)
            
            if cursor.rowcount == 1:
                count_inserted += 1
            elif cursor.rowcount == 2:
                count_updated += 1
            
            conn.commit()

        except Error as e:
            count_errors += 1
            conn.rollback()
        except Exception as e:
            count_errors += 1
            print(f"\n❌ Erreur Python inattendue pour {voiture.get('url')}: {e}")
            conn.rollback()

    cursor.close()
    print("\n--- Intégration terminée ---")
    print(f"✅ Nouveaux véhicules insérés : {count_inserted}")
    print(f"🔄 Véhicules mis à jour : {count_updated}")
    print(f"❌ Lignes en erreur (ignorées) : {count_errors}")
    print(f"Total traité : {count_inserted + count_updated + count_errors}")


def run_database_pipeline():
    """
    Point d'entrée principal pour le pipeline de la BDD.
    Se connecte, crée la BDD/schéma, et intègre les données.
    """
    conn = None
    try:
        # 1. Se connecter SANS BDD pour la créer
        conn_init = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor_init = conn_init.cursor()
        cursor_init.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor_init.close()
        conn_init.close()
        
        # 2. Se connecter à la BDD spécifique
        conn = mysql.connector.connect(**DB_CONFIG)
        print(f"Connexion à la base de données '{DB_CONFIG['database']}' réussie.")
        
        # 3. Lancer l'intégration
        integrer_donnees(conn)

    except Error as e:
        print(f"❌ ERREUR de connexion à MySQL: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Connexion MySQL fermée.")
