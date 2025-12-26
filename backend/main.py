from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import StringIO

# ✅ IMPORT YOUR DATABASE CONNECTION
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

# -------------------- VALIDATION CONFIG --------------------
REQUIRED_COLUMNS = [
    "title", "description", "BFC_message",
    "la", "ld", "nf", "nd", "ns", "ent",
    "revd", "self"
    # Note: 'Label' is optional now
]

NUMERIC_COLUMNS = ["la", "ld", "nf", "nd", "ns", "ent"]

# -------------------- API --------------------
@app.post("/api/predict")
def upload_csv(file: UploadFile = File(...)):

    # 1️⃣ Read CSV
    try:
        content = file.file.read().decode("utf-8")
        df = pd.read_csv(StringIO(content))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid CSV")

    # 2️⃣ Validate columns (Label optional)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

    # If Label not in CSV, create it with None
    if "Label" not in df.columns:
        df["Label"] = None

    # 3️⃣ Validate numeric columns
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4️⃣ Boolean conversion
    df["revd"] = df["revd"].astype(bool)
    df["self"] = df["self"].astype(bool)
    df = df.fillna("")

    # 5️⃣ Insert RAW data
    conn = get_connection()
    cur = conn.cursor()
    raw_ids = []

    try:
        for _, r in df.iterrows():
            cur.execute("""
                INSERT INTO raw_validated_data
                (title, description, bfc_message,
                 la, ld, nf, nd, ns, ent,
                 revd, self, label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                r["title"], r["description"], r["BFC_message"],
                float(r["la"]), float(r["ld"]), float(r["nf"]),
                float(r["nd"]), float(r["ns"]), float(r["ent"]),
                bool(r["revd"]), bool(r["self"]), r["Label"]
            ))
            raw_ids.append(cur.fetchone()["id"])

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    # 6️⃣ Text preprocessing
    df["clean_text"] = (
        df["title"].astype(str) + " " +
        df["description"].astype(str) + " " +
        df["BFC_message"].astype(str)
    )

    # 7️⃣ Numeric preprocessing
    df["revd_norm"] = df["revd"].apply(lambda x: 1 if x else 0)
    df["self_norm"] = df["self"].apply(lambda x: 1 if x else 0)

    x = df[NUMERIC_COLUMNS].fillna(0)
    x_norm = (x - x.min()) / (x.max() - x.min())
    x_norm = x_norm.fillna(0).astype(float)

    # 8️⃣ Insert PROCESSED data
    try:
        for i, r in df.iterrows():
            cur.execute("""
                INSERT INTO processed_features_data
                (raw_id, clean_text,
                 la_norm, ld_norm, nf_norm, nd_norm,
                 ns_norm, ent_norm, revd_norm, self_norm, label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                raw_ids[i],
                r["clean_text"],
                float(x_norm.loc[i, "la"]),
                float(x_norm.loc[i, "ld"]),
                float(x_norm.loc[i, "nf"]),
                float(x_norm.loc[i, "nd"]),
                float(x_norm.loc[i, "ns"]),
                float(x_norm.loc[i, "ent"]),
                int(r["revd_norm"]),
                int(r["self_norm"]),
                r["Label"]
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()

    return {"status": "success", "rows": len(df)}
