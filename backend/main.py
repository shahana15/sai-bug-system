


from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import StringIO
from database import get_connection

app = FastAPI()

# -------------------- CORS --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- CONFIG --------------------
REQUIRED_COLUMNS = [
    "title", "description", "BFC_message",
    "la", "ld", "nf", "nd", "ns", "ent",
    "revd", "self"
]

NUMERIC_COLUMNS = ["la", "ld", "nf", "nd", "ns", "ent"]

FEATURE_MIN_MAX = {
    "la": (0.0, 4845.0),
    "ld": (0.0, 8081.0),
    "nf": (1.0, 78.0),
    "nd": (1.0, 38.0),
    "ns": (1.0, 3.0),
    "ent": (0.0, 1.0),
}

# -------------------- API --------------------
@app.post("/api/predict")
def upload_csv(file: UploadFile = File(...)):

    # 1️⃣ Read CSV safely (Excel-proof)
    try:
        content = file.file.read().decode("utf-8-sig")
        df = pd.read_csv(StringIO(content))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid CSV file")

    # 2️⃣ Validate required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

    # 3️⃣ Reject NULLs (USER CANNOT SEND NULL)
    if df[REQUIRED_COLUMNS].isnull().any().any():
        raise HTTPException(
            status_code=422,
            detail="CSV contains NULL / empty values. Please fix and re-upload."
        )

    # 4️⃣ Validate numeric columns
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise")

    # 5️⃣ Validate booleans
    df["revd"] = df["revd"].astype(bool)
    df["self"] = df["self"].astype(bool)

    # -------------------- DB INSERT (RAW) --------------------
    conn = get_connection()
    cur = conn.cursor()
    raw_ids = []

    try:
        for _, r in df.iterrows():
            cur.execute("""
                INSERT INTO raw_validated_data
                (title, description, bfc_message,
                 la, ld, nf, nd, ns, ent,
                 revd, self)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                r["title"], r["description"], r["BFC_message"],
                r["la"], r["ld"], r["nf"],
                r["nd"], r["ns"], r["ent"],
                r["revd"], r["self"]
            ))
            raw_ids.append(cur.fetchone()["id"])

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    # -------------------- TEXT PREPROCESS --------------------
    df["clean_text"] = (
        df["title"].astype(str) + " " +
        df["description"].astype(str) + " " +
        df["BFC_message"].astype(str)
    )

    # -------------------- NON-TEXT PREPROCESS (BHANDARI) --------------------
    for col in NUMERIC_COLUMNS:
        min_v, max_v = FEATURE_MIN_MAX[col]
        df[f"{col}_norm"] = (df[col] - min_v) / (max_v - min_v)

    df["revd_norm"] = df["revd"].apply(lambda x: 1 if x else 0)
    df["self_norm"] = df["self"].apply(lambda x: 1 if x else 0)

    # -------------------- DB INSERT (PROCESSED) --------------------
    try:
        for i, r in df.iterrows():
            cur.execute("""
                INSERT INTO processed_features_data
                (raw_id, clean_text,
                 la_norm, ld_norm, nf_norm,
                 nd_norm, ns_norm, ent_norm,
                 revd_norm, self_norm)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                raw_ids[i],
                r["clean_text"],
                r["la_norm"], r["ld_norm"], r["nf_norm"],
                r["nd_norm"], r["ns_norm"], r["ent_norm"],
                r["revd_norm"], r["self_norm"]
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()

    return {"status": "success", "rows_inserted": len(df)}
