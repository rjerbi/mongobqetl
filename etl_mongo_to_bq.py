import pandas as pd  
from pymongo import MongoClient
from google.cloud import bigquery
from google.oauth2 import service_account
from dateutil import parser


# Configuration de base des données Mongo et BigQuery
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "PFE"
COLLECTION_ESOS = "esos"
COLLECTION_ESIS = "esis"

# Chemin de la clé de service pour BigQuery
KEY_PATH = r"C:\Users\rahma\Downloads\ETL\dwh-esi-project-01-620983b03788.json"
BQ_PROJECT = "dwh-esi-project-01" 
BQ_DATASET_ESI = "esi_dataset"
BQ_DATASET_ESO = "eso_dataset"
BQ_TABLE_ESOS = "esos"
BQ_TABLE_ESIS = "esis"


# Initialisation du client BigQuery avec les identifiants
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Connexion avec la base MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]


# Fonction pour parser les dates en format datetime
def parse_date(d):
    if pd.notnull(d):
        try:
            return parser.parse(d)
        except Exception:
            return None
    return None

# Fonction pour calculer la durée du bail (en jours)
def calculate_bail_duration(debut, fin):
    if debut and fin:
        return (fin - debut).days
    return None


# Conversion des ObjectId et autres objets MongoDB en types compatibles
def convert_objectid(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x) if 'ObjectId' in str(type(x)) else x)
            try:
                df[col] = df[col].astype(float)
            except:
                pass
    return df

# Extraction des données depuis MongoDB et transformation en DataFrame
def extract_collection(collection_name):
    collection = db[collection_name]
    data = list(collection.find())  # récupérer tous les documents
    df = pd.json_normalize(data)    # normaliser les données imbriquées
    df = convert_objectid(df)
    return df

# Extraction des données pour les collections ESOS et ESIS
esos_df = extract_collection(COLLECTION_ESOS)
esis_df = extract_collection(COLLECTION_ESIS)

# Nettoyage et enrichissement des données ESIS
def clean_and_enrich_esis(df):
    # Transformation des colonnes de type date
    for col in ['dateDebutBail.$date', 'dateFinBail.$date', 'datePriseBail.$date', 'dateResiliation.$date']:
        if col in df.columns:
            df[col.replace('.$date','')] = df[col].apply(parse_date)

    # Calcul des colonnes dérivées liées aux baux
    if 'dateDebutBail' in df.columns and 'dateFinBail' in df.columns:
        df['duree_bail_jours'] = df.apply(lambda row: calculate_bail_duration(row.get('dateDebutBail'), row.get('dateFinBail')), axis=1)
        df['annee_debut'] = df['dateDebutBail'].dt.year
        df['mois_debut'] = df['dateDebutBail'].dt.month

    # Calcul de l’écart entre engagé et facturé
    if 'engageTTC' in df.columns and 'factureTTC' in df.columns:
        df['ecart_engage_facture'] = df['engageTTC'].astype(float) - df['factureTTC'].astype(float)

    # Suppression des colonnes techniques inutiles ($oid, $date, etc.)
    drop_cols = [col for col in df.columns if '$oid' in col or '$date' in col or col.startswith('__')]
    df = df.drop(columns=drop_cols, errors='ignore')
    return df

# Nettoyage et enrichissement des données ESOS
def clean_and_enrich_esos(df):
    for col in ['createdAt.$date', 'updatedAt.$date']:
        if col in df.columns:
            df[col.replace('.$date','')] = df[col].apply(parse_date)

    # Suppression des colonnes techniques inutiles
    drop_cols = [col for col in df.columns if '$oid' in col or '$date' in col or col.startswith('__')]
    df = df.drop(columns=drop_cols, errors='ignore')
    return df

# Application du nettoyage
esis_clean = clean_and_enrich_esis(esis_df)
esos_clean = clean_and_enrich_esos(esos_df)

# Vérification des nouvelles lignes à charger (évite les doublons dans BigQuery)
def filter_new_rows(df, dataset_name, table_name, key_column='_id'):
    table_id = f"{BQ_PROJECT}.{dataset_name}.{table_name}"
    query = f"SELECT {key_column} FROM `{table_id}`"
    try:
        existing_keys = bq_client.query(query).to_dataframe()
        df_new = df[~df[key_column].isin(existing_keys[key_column])]
    except Exception:
        # si la table n'existe pas encore, on charge toutes les données
        df_new = df
    return df_new

# Filtrage des données nouvelles
esis_to_load = filter_new_rows(esis_clean, BQ_DATASET_ESI, BQ_TABLE_ESIS)
esos_to_load = filter_new_rows(esos_clean, BQ_DATASET_ESO, BQ_TABLE_ESOS)

# Chargement des données nettoyées et enrichies dans BigQuery
def load_to_bigquery(df, dataset_name, table_name):
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)  # conversion en string pour compatibilité
    table_id = f"{BQ_PROJECT}.{dataset_name}.{table_name}"
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()  # attendre la fin du job
    print(f"Chargé {len(df)} lignes dans {table_id}")

# Charger uniquement les nouvelles lignes
if not esis_to_load.empty:
    load_to_bigquery(esis_to_load, BQ_DATASET_ESI, BQ_TABLE_ESIS)
if not esos_to_load.empty:
    load_to_bigquery(esos_to_load, BQ_DATASET_ESO, BQ_TABLE_ESOS)

print("ETL terminé avec succès !")
