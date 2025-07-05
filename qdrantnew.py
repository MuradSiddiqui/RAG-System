# Qdrant Semantic Search Setup with Optional BM25
import pandas as pd
import re
import spacy
import os
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer
from langdetect import detect, DetectorFactory
from rank_bm25 import BM25Okapi

# Seed for language detection
DetectorFactory.seed = 0

# Configuration
ENABLE_BM25 = True
DATASET_PATH = "/Users/muradsiddiqui/Desktop/dataset/RAG_System/2000K_final_extended_double_data.csv"
COLLECTION_EN = "doubles_semantic"
COLLECTION_DE = "doubles_semantic_de"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Qdrant Setup
qdrant_client = QdrantClient(
    url="https://42fb40a4-e46d-4d7f-96d3-6b4e9309cc4b.europe-west3-0.gcp.cloud.qdrant.io:6333",
    api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.a-Gbv6aEgZWAG6eoKRhaAQsklfA5Oa_gtaAcX_R9HTg",
    timeout=120.0
)

#  NLP Setup 
nlp_en = spacy.load("en_core_web_sm")
nlp_de = spacy.load("de_core_news_sm")
model = SentenceTransformer(EMBEDDING_MODEL)

#  Utils 
def clean_text(text):
    text = re.sub(r'[^\x00-\x7F]+', '', str(text))
    text = re.sub(r'[^\w\s]', '', text)
    return text.lower()

def safe_detect_lang(text):
    try:
        if len(text.strip()) <= 5:
            probable_german = ['gerne', 'gut', 'danke', 'bitte', 'ja', 'nein', 'nicht']
            if text.lower() in probable_german:
                return 'de'
        lang = detect(text)
        return lang if lang in ['en', 'de'] else 'en'
    except:
        return 'en'

def inject_negation(text, lang="en"):
    nlp = nlp_en if lang == "en" else nlp_de
    doc = nlp(text)
    negated_heads = {tok.head.i for tok in doc if tok.dep_ == "neg" or tok.text.lower() in {"not", "no", "never", "none", "n't", "nicht", "kein", "keine", "keinen", "keiner", "keines"}}
    return " ".join(["NEG_" + tok.text if i in negated_heads else tok.text for i, tok in enumerate(doc)])

# Load Data 
df = pd.read_csv(DATASET_PATH, sep=";", engine="python", on_bad_lines='skip')
df['description_en'] = df['description_en'].apply(clean_text)
df['description_de'] = df['description_de'].apply(clean_text)
df['tags_en'] = df['tags_en'].fillna("").apply(lambda x: " ".join(re.split(r'[;,]', x))).str.lower()
df['tags_de'] = df['tags_de'].fillna("").apply(lambda x: " ".join(re.split(r'[;,]', x))).str.lower()

# Negation + Embedding
df['negation_description_en_final'] = df['description_en'].apply(lambda x: inject_negation(x, lang='en'))
df['negation_description_de_final'] = df['description_de'].apply(lambda x: inject_negation(x, lang='de'))
df['embedding_en'] = df['negation_description_en_final'].apply(lambda x: model.encode(x).tolist())
df['embedding_de'] = df['negation_description_de_final'].apply(lambda x: model.encode(x).tolist())

# Create Qdrant Collections
vector_size = len(df['embedding_en'].iloc[0])
for name in [COLLECTION_EN, COLLECTION_DE]:
    if not qdrant_client.collection_exists(name):
        qdrant_client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
        )

# Upsert Points to Qdrant
points_en, points_de = [], []
for idx, row in df.iterrows():
    point_id = int(row['unique_identifier']) if not pd.isnull(row['unique_identifier']) else idx
    payload = {
        "description_en": row['description_en'],
        "description_de": row['description_de'],
        "tags_en": row['tags_en'],
        "tags_de": row['tags_de'],
        "negation_description_en_final": row['negation_description_en_final'],
        "negation_description_de_final": row['negation_description_de_final'],
        "has_negation_en": "NEG_" in row['negation_description_en_final'],
        "has_negation_de": "NEG_" in row['negation_description_de_final'],
        "uid": row.get("unique_identifier", None),
        "double_id": row.get("double_id", None)

    }
    points_en.append(PointStruct(id=point_id, vector=row['embedding_en'], payload=payload))
    points_de.append(PointStruct(id=point_id, vector=row['embedding_de'], payload=payload))

#Upload in Batches to avoiud timeout error
def upload_in_batches(collection_name, points, batch_size=200):
    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]
        qdrant_client.upsert(collection_name=collection_name, points=batch)
        print(f"Uploaded batch {i // batch_size + 1} ({len(batch)} points)")

# Upload in Batches to Qdrant
upload_in_batches(COLLECTION_EN, points_en)
upload_in_batches(COLLECTION_DE, points_de)
print("All data uploaded to Qdrant.")


# BM25 Setup (Optional)
if ENABLE_BM25:
    tokenized_tags_en = [str(tags).split() for tags in df['tags_en']]
    tokenized_tags_de = [str(tags).split() for tags in df['tags_de']]
    bm25_en = BM25Okapi(tokenized_tags_en)
    bm25_de = BM25Okapi(tokenized_tags_de)

# Search Function 
def search_query(query_text):
    lang = safe_detect_lang(query_text)
    print(f"🔍 Detected language: {lang}")

    bm25 = bm25_de if lang == 'de' else bm25_en
    collection_name = COLLECTION_DE if lang == 'de' else COLLECTION_EN
    tags_column = 'tags_de' if lang == 'de' else 'tags_en'
    desc_column = 'description_de' if lang == 'de' else 'description_en'

    if ENABLE_BM25:
        query_tokens = query_text.lower().split()
        scores = bm25.get_scores(query_tokens)
        top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:5]
        for i in top_indices:
            print(f"BM25 Match: {df.iloc[i][tags_column]} (Score: {scores[i]:.4f})")

    # Qdrant Semantic Search
    query_vector = model.encode(query_text).tolist()
    results = qdrant_client.search(collection_name=collection_name, query_vector=query_vector, limit=5)
    print("\n Qdrant Semantic Results:")
    for res in results:
        print(f"Score: {res.score:.4f} | Text: {res.payload.get(desc_column)}")


if __name__ == "__main__":
    while True:
        query = input("\nEnter a search query (or 'exit'): ")
        if query.lower() == 'exit':
            break
        search_query(query)
