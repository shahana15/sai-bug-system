from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import io
from database import get_connection

app = FastAPI()

# Required columns in CSV
REQUIRED_COLUMNS = [
    "title", "description", "BFC_message", "la", "ld", "nf", "nd", "ns", "ent",
    "revd", "self", "label"
]

NUMERIC_COLUMNS = ["la", "ld", "nf", "nd", "ns", "ent", "label"]

BOOLEAN_COLUMNS = ["revd", "self"]

@app.post("/api/predict")
async def predict(file: UploadFile = File(...)):
    # Check if CSV
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be CSV")

    # Read CSV into DataFrame
    content = await file.read()
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV parse error: {str(e)}")

    # Validate required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing_cols}"
        )

    # Filter only required columns
    df = df[REQUIRED_COLUMNS]

    # Validate numeric columns
    for col in NUMERIC_COLUMNS:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise HTTPException(status_code=400, detail=f"Column {col} must be numeric")

    # Validate boolean columns (convert TRUE/FALSE to bool)
    for col in BOOLEAN_COLUMNS:
        df[col] = df[col].apply(lambda x: True if str(x).upper() in ["TRUE", "1"] else False)

    # --- Table 1: raw_validated_data ---
    try:
        conn = get_connection()
        cur = conn.cursor()

        raw_ids = []
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO raw_validated_data
                (title, description, bfc_message, la, ld, nf, nd, ns, ent, revd, self, label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                row["title"], row["description"], row["BFC_message"],
                row["la"], row["ld"], row["nf"], row["nd"], row["ns"], row["ent"],
                row["revd"], row["self"], row["label"]
            ))
            raw_id = cur.fetchone()[0]
            raw_ids.append(raw_id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"DB insert error (raw): {str(e)}")

    # --- Table 2: processed_features_data ---
    try:
        # Concatenate text columns
        df["processed_text"] = df["title"].astype(str) + " " + df["description"].astype(str) + " " + df["BFC_message"].astype(str)

        # Normalize numeric columns
        df_num = df[NUMERIC_COLUMNS].fillna(0)
        df_norm = (df_num - df_num.min()) / (df_num.max() - df_num.min())
        for col in df_norm.columns:
            df[col + "_norm"] = df_norm[col]

        # Convert boolean to int for DB insert
        for col in BOOLEAN_COLUMNS:
            df[col + "_norm"] = df[col].apply(lambda x: 1 if x else 0)

        # Insert into processed_features_data
        for idx, row in df.iterrows():
            raw_id = raw_ids[idx]
            cur.execute("""
                INSERT INTO processed_features_data
                (raw_id, processed_text, la_norm, ld_norm, nf_norm, nd_norm, ns_norm, ent_norm, revd_norm, self_norm, label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                raw_id, row["processed_text"],
                row["la_norm"], row["ld_norm"], row["nf_norm"], row["nd_norm"], row["ns_norm"], row["ent_norm"],
                row["revd_norm"], row["self_norm"], row["label"]
            ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"DB insert error (processed): {str(e)}")

    return {"results": [{"row": i+1, "status": "success"} for i in range(len(df))]}
