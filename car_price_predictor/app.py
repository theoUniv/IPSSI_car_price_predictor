import subprocess
import sys
import os

try:
    from database import database
except ImportError:
    print("❌ ERREUR: Le fichier 'database.py' est introuvable.")
    sys.exit(1)

# --- Configuration des chemins vers les scripts ---
CONVERTER_SCRIPT = os.path.join("converter", "JsonToCsv.py")
MODEL_SCRIPT = os.path.join("models", "model.py")


def run_external_script(script_path):
    """
    Exécute un script Python externe en tant que sous-processus.
    Cela garantit qu'il s'exécute avec son propre contexte.
    """
    # Utilise l'exécutable Python actuel pour lancer le script
    python_executable = sys.executable 
    
    print(f"--- Exécution de '{script_path}' ---")
    
    try:
        # check=True lève une erreur si le script échoue (code de sortie non nul)
        # capture_output=True capture stdout/stderr
        # text=True décode stdout/stderr en texte (utf-8)
        result = subprocess.run(
            [python_executable, script_path], 
            check=True, 
            capture_output=True, 
            text=True,
            encoding='utf-8' # Forcer l'encodage
        )
        
        if result.stdout:
            print(result.stdout)
            
        print(f"--- '{script_path}' terminé avec succès ---")
        return True
        
    except FileNotFoundError:
        print(f"❌ ERREUR: Script introuvable à '{script_path}'")
        return False
    except subprocess.CalledProcessError as e:
        print(f"❌ ERREUR lors de l'exécution de '{script_path}':")
        print(e.stderr) 
        return False
    except Exception as e:
        print(f"❌ ERREUR inattendue avec '{script_path}': {e}")
        return False

def main_pipeline():
    """
    Orchestre l'ensemble du pipeline de données.
    """
    print("🚀 Démarrage du pipeline de données complet...")
    
    # ÉTAPE 1: Conversion JSON vers CSV
    print("\n[ÉTAPE 1/3] Conversion JSON vers CSV...")
    if not run_external_script(CONVERTER_SCRIPT):
        print("🛑 Échec de l'étape 1. Arrêt du pipeline.")
        return

    # ÉTAPE 2: Chargement du JSON dans la BDD MySQL
    print("\n[ÉTAPE 2/3] Chargement des données JSON dans MySQL...")
    try:
        database.run_database_pipeline()
        print("--- Étape 2 terminée avec succès ---")
    except Exception as e:
        print(f"❌ ERREUR lors du chargement dans la base de données: {e}")
        print("🛑 Échec de l'étape 2. Arrêt du pipeline.")
        return

    # ÉTAPE 3: Entraînement du modèle
    print("\n[ÉTAPE 3/3] Entraînement du modèle de prédiction...")
    if not run_external_script(MODEL_SCRIPT):
        print("🛑 Échec de l'étape 3. Le pipeline est terminé avec des erreurs.")
        return

    print("\n🎉 Pipeline complet terminé avec succès ! 🎉")

if __name__ == "__main__":
    main_pipeline()
