# %%
import pandas as pd
import numpy as np

# %%
# ==========================================
# 1. CONFIGURATION ET CONSTANTES
# ==========================================
INPUT_PATH = './data/fr.openfoodfacts.org.products.csv.gz'
OUTPUT_PATH = './data/openfoodfacts_clean.csv'

# Colonnes d'identification produit
COLS_IDENTITY = [
    'code',               # Clé unique (Code-barres)
    'product_name',       # Nom d'affichage
    'generic_name',       # Nom générique
    'brands',             # Marque
    'quantity',           # Quantité (ex: 500g)
    'image_small_url',    # URL image légère
    'categories_tags'     # Catégorisation pour recherche
]

# Colonnes nutritionnelles (pour 100g)
COLS_NUTRITION = [
    'energy-kcal_100g',
    'fat_100g',
    'sugars_100g',
    'saturated-fat_100g',
    'carbohydrates_100g',
    'fiber_100g',
    'proteins_100g',
    'salt_100g'
]

COLS_DIETS = [
    'labels_tags',               # Les certifications (Bio, Vegan officiel...)
    'ingredients_analysis_tags', # L'analyse algo (Sans huile de palme, Vegan déduit...)
    'allergens',                 # Le "Kill Switch" de sécurité
    'traces'                     # Pour les allergies très strictes (optionnel mais conseillé)
]

COLS_SCORE = [
    'nutriscore_grade',          # A, B, C, D, E
    'nova_group',                # 1, 2, 3, 4 (Transformation)
    'environmental_score_grade'  # Eco-Score (Impact planète)
]


# Liste finale des colonnes à extraire
SELECTED_COLS = COLS_IDENTITY + COLS_NUTRITION


# %%
# ==========================================
# 2. FONCTIONS DE TRAITEMENT
# ==========================================
def process_data_in_chunks(file_path, cols, chunk_size=10000):
    """
    Lit un fichier CSV volumineux par blocs, filtre les colonnes 
    et supprime les valeurs manquantes (NaN) à la volée.
    """
    chunks = pd.read_csv(
        file_path,
        compression='gzip',
        sep='\t',
        on_bad_lines='skip',
        chunksize=chunk_size,
        low_memory=False,
        usecols=cols  # Optimisation : On ne charge que ce qui est nécessaire
    )

    clean_chunks = []
    
    print("Traitement des blocs en cours...")
    for chunk in chunks:
        # Suppression immédiate des lignes incomplètes pour économiser la mémoire
        chunk_clean = chunk.dropna()
        if not chunk_clean.empty:
            clean_chunks.append(chunk_clean)
            
    return pd.concat(clean_chunks, ignore_index=True)


# %%
# ==========================================
# 3. EXÉCUTION DU PIPELINE
# ==========================================

# A. Chargement et nettoyage
df = process_data_in_chunks(INPUT_PATH, SELECTED_COLS)

# B. Gestion des doublons
nb_doublons = df.duplicated(subset=['code']).sum()
print(f"Doublons détectés : {nb_doublons}")

if nb_doublons > 0:
    df = df.drop_duplicates(subset=['code'])
    print("Doublons supprimés.")
else:
    print("Aucun doublon à supprimer.")

# C. Aperçu et Export
print(f"Lignes restantes : {len(df)}")
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print(f"Fichier sauvegardé : {OUTPUT_PATH}")


