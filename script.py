# %%
import pandas as pd

INPUT_PATH = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"

OUTPUT_PATH = './data/openfoodfacts_clean.csv'

# Colonnes minimales pour le calcul du Nutri-Score
nutriscore_cols = [
    'energy_100g', 'fat_100g', 'saturated-fat_100g', 
    'sugars_100g', 'fiber_100g', 'proteins_100g', 'carbohydrates_100g',
    'salt_100g'
]

cols_diets =['allergens']

target = ['nutriscore_grade']
identity_cols = ['code', 'product_name']

cols = nutriscore_cols + target + identity_cols +cols_diets 


def process_data(file_path, cols, chunk_size=10000):
    reader = pd.read_csv(
        file_path, compression='gzip', sep='\t',
        on_bad_lines='skip', chunksize=chunk_size,
        low_memory=False, usecols=cols
    )

    clean_chunks = []

    for chunk in reader:
        # 1. On travaille sur une copie du bloc actuel
        temp_chunk = chunk.copy()

        # 2. On élimine d'abord les lignes sans Nutri-Score (Indispensable)
        temp_chunk = temp_chunk.dropna(subset=['nutriscore_grade'])
        temp_chunk = temp_chunk[temp_chunk['nutriscore_grade'].isin(['a', 'b', 'c', 'd', 'e'])]

        # 3. Conversion numérique et remplissage des NaN par 0
        for col in nutriscore_cols:
            temp_chunk[col] = pd.to_numeric(temp_chunk[col], errors='coerce')
        
        temp_chunk[nutriscore_cols] = temp_chunk[nutriscore_cols].fillna(0)

        # 4. Filtres pour eliminer les outliers (0-100g et Energie)
        for col in nutriscore_cols:
            if col != 'energy_100g':
                temp_chunk = temp_chunk[(temp_chunk[col] >= 0) & (temp_chunk[col] <= 100)]
        
        temp_chunk = temp_chunk[(temp_chunk['energy_100g'] >= 0) & (temp_chunk['energy_100g'] < 4000)]

        # 5. fill the na in product names
        temp_chunk['product_name'] = temp_chunk['product_name'].fillna('Unknown Product')
        temp_chunk['allergens'] = temp_chunk['allergens'].fillna('None')
        # 6 Detection et suppression des doublons
        nb_doublons = temp_chunk.duplicated(subset=['code']).sum()

        if nb_doublons > 0:
            temp_chunk = temp_chunk.drop_duplicates(subset=['code'])


        # 7. AJOUTER le bloc nettoyé à la liste 
        if not temp_chunk.empty:
            clean_chunks.append(temp_chunk)

        df = pd.concat(clean_chunks, ignore_index=True)
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"Fichier sauvegardé : {OUTPUT_PATH}")

    return df




# %%
process_data(INPUT_PATH, cols)


