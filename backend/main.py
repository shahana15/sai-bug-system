# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from database import get_connection
from datetime import datetime
import re
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from gensim.parsing.preprocessing import remove_stopwords

# Download nltk resources
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

# FastAPI app
app = FastAPI(title="BIC Prediction API")

# CORS for React frontend
origins = ["http://localhost:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Columns
REQUIRED_COLUMNS = [
    "title","description","BFC_message",
    "la","ld","nf","nd","ns","ent",
    "revd","self"
]

NUMERIC_COLS = ["la","ld","nf","nd","ns","ent"]
BOOLEAN_COLS = ["revd","self"]
TEXT_COLS = ["title","description","BFC_message"]

# -------------------------------
# Text Preprocessing
# -------------------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "<URL>", text)
    text = re.sub(r"\d+", "", text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = ' '.join(text.split())
    text = remove_stopwords(text)
    return text

def lemmatize_text(text):
    tokens = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()
    lemmatized = []
    for word, tag in nltk.pos_tag(tokens):
        pos = tag[0].upper()
        pos_dict = {"J": wordnet.ADJ, "N": wordnet.NOUN, "V": wordnet.VERB, "R": wordnet.ADV}
        lemmatized.append(lemmatizer.lemmatize(word, pos_dict.get(pos, wordnet.NOUN)))
    return ' '.join(lemmatized)

def preprocess_text_columns(df):
    df["clean_text"] = df[TEXT_COLS].fillna("").agg(" ".join, axis=1)
    df["clean_text"] = df["clean_text"].apply(clean_text).apply(lemmatize_text)
    return df

# -------------------------------
# Numeric + Boolean Preprocessing
# -------------------------------
def preprocess_numeric(df):
    df_proc = df.copy()
    # Boolean columns: convert to int
    for col in BOOLEAN_COLS:
        df_proc[col] = df_proc[col].apply(lambda x: 1 if x==True else 0)
    # Fill NaN with 0
    df_proc[NUMERIC_COLS + BOOLEAN_COLS] = df_proc[NUMERIC_COLS + BOOLEAN_COLS].fillna(0)
    # Normalize using min-max
    df_proc_norm = df_proc.copy()
    for col in NUMERIC_COLS + BOOLEAN_COLS:
        min_val = df_proc[col].min()
        max_val = df_proc[col].max()
        if max_val - min_val == 0:
            df_proc_norm[col + "_norm"] = 0
        else:
            df_proc_norm[col + "_norm"] = (df_proc[col] - min_val) / (max_val - min_val)
    return df_proc_norm

# -------------------------------
# /api/predict route
# -------------------------------
@app.post("/api/predict")
async def predict(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid CSV file")

    # Input validation: check required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing_cols}")

    # Ensure numeric columns are numeric
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Ensure boolean columns are boolean or 0/1
    for col in BOOLEAN_COLS:
        df[col] = df[col].map({True: 1, False: 0}).fillna(0)

    # Preprocess
    processed_df = preprocess_text_columns(df)
    processed_df = preprocess_numeric(processed_df)

    # Save to PostgreSQL
    conn = get_connection()
    cur = conn.cursor()
    try:
        for idx, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw_validated_data
                (title, description, bfc_message, la, ld, nf, nd, ns, ent, revd, self, label, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                row["title"], row["description"], row["BFC_message"],
                row.get("la",0), row.get("ld",0), row.get("nf",0), row.get("nd",0),
                row.get("ns",0), row.get("ent",0), row.get("revd",0), row.get("self",0),
                row.get("BIC", None), datetime.now()
            ))
            raw_id = cur.fetchone()[0]

            # Insert processed
            cur.execute("""
                INSERT INTO processed_features_data
                (raw_id, clean_text, la_norm, ld_norm, nf_norm, nd_norm, ns_norm, ent_norm, revd_norm, self_norm, label, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                raw_id, processed_df.loc[idx,"clean_text"],
                processed_df.loc[idx,"la_norm"], processed_df.loc[idx,"ld_norm"],
                processed_df.loc[idx,"nf_norm"], processed_df.loc[idx,"nd_norm"],
                processed_df.loc[idx,"ns_norm"], processed_df.loc[idx,"ent_norm"],
                processed_df.loc[idx,"revd_norm"], processed_df.loc[idx,"self_norm"],
                row.get("BIC", None), datetime.now()
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

    # Dummy prediction
    results = []
    for i in range(len(df)):
        results.append({
            "row": i,
            "prediction": 1,
            "confidence": 0.85,
            "explanation": [
                "High code churn increased risk",
                "Commit message complexity contributed",
                "Low developer experience"
            ]
        })

    return {
        "total_records": len(df),
        "results": results
    }
