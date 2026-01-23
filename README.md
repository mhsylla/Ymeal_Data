Voici un `README.md` concis et adapté à votre structure de projet (qui utilise `uv` pour les dépendances) :

# YMeal Data Processor

Outils de nettoyage et de structuration des données OpenFoodFacts.

## 🛠 Prérequis & Installation

Ce projet utilise [uv](https://github.com/astral-sh/uv) pour la gestion des dépendances.

### Installer uv (Windows)
Ouvrez PowerShell et exécutez :
```powershell
powershell -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"

```

### Installer le projet

```bash
# 1. Installer les dépendances (crée le .venv automatiquement)
uv sync

```

## 📂 Préparation des données

1. Télécharger la base de données brute OpenFoodFacts (`fr.openfoodfacts.org.products.csv.gz`).
2. Placer le fichier dans le dossier `data/`.


## 📦 Structure

* `data/` : Données brutes et nettoyées (ignoré par git sauf `.keep`).
* `index.ipynb` : Notebook de prototypage et nettoyage.
* `main.py` : Script d'exécution final.


