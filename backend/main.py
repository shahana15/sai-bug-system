# import logging
# from fastapi import FastAPI, UploadFile, File, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# import pandas as pd
# from io import StringIO
# from database import get_connection
# import pickle
# from pathlib import Path
# from gensim.models import FastText
# import numpy as np
# import traceback

# # -------------------- LOGGER --------------------
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # -------------------- APP --------------------
# app = FastAPI()

# # -------------------- CORS CONFIG --------------------
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "http://localhost:3000",
#         "http://127.0.0.1:3000"
#     ],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # -------------------- CONFIG --------------------
# REQUIRED_COLUMNS = [
#     "title", "description", "BFC_message",
#     "la", "ld", "nf", "ns", "ent",
#     "nrev", "rtime", "ndev", "age", "aexp", "asawr",
#     "revd", "bugcount", "fixcount"
# ]

# NUMERIC_COLUMNS = [
#     "la", "ld", "nf", "ns", "ent",
#     "nrev", "rtime", "ndev", "age", "aexp", "asawr",
#     "bugcount", "fixcount"
# ]

# BOOLEAN_COLUMNS = ["revd"]
# NUMERIC_COLUMNS_PLUS_BOOLEAN = NUMERIC_COLUMNS + BOOLEAN_COLUMNS  # For feature stacking
# LABEL_MAP = {0: "Extrinsic Bug", 1: "Intrinsic Bug"}

# # -------------------- LOAD MODELS --------------------
# SVM_MODEL_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\FastText_SVM.pkl")
# FASTTEXT_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\fasttext.model")

# with open(SVM_MODEL_PATH, "rb") as f:
#     svm_model = pickle.load(f)
# logger.info("✅ SVM model loaded successfully")

# ft_model = FastText.load(str(FASTTEXT_PATH))
# logger.info("✅ FastText model loaded successfully")

# expected_features = ft_model.vector_size + len(NUMERIC_COLUMNS_PLUS_BOOLEAN)
# logger.info(f"Expected features for SVM: {expected_features}")

# def fasttext_vector(text: str) -> np.ndarray:
#     words = str(text).split()
#     vectors = [ft_model.wv[word] for word in words if word in ft_model.wv]
#     return np.mean(vectors, axis=0) if vectors else np.zeros(ft_model.vector_size)

# # -------------------- API --------------------
# @app.post("/api/predict")
# def upload_csv(file: UploadFile = File(...)):
#     conn, cur = None, None
#     debug_messages = []  # Collect debug logs for frontend
#     try:
#         # Read CSV
#         content = file.file.read().decode("utf-8-sig")
#         df = pd.read_csv(StringIO(content))
#         msg = f"CSV loaded with {len(df)} rows"
#         logger.info(msg)
#         debug_messages.append(msg)

#         # Validate columns
#         missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
#         if missing:
#             raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

#         # Reject NULLs
#         if df[REQUIRED_COLUMNS].isnull().any().any():
#             raise HTTPException(status_code=422, detail="CSV contains NULL values")

#         # Numeric conversion
#         for col in NUMERIC_COLUMNS:
#             df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

#         # Boolean conversion
#         for col in BOOLEAN_COLUMNS:
#             df[col] = df[col].apply(lambda x: 1 if str(x).upper() == "TRUE" else 0)

#         # -------------------- INSERT RAW DATA --------------------
#         conn = get_connection()
#         cur = conn.cursor()
#         raw_ids = []

#         for _, r in df.iterrows():
#             cur.execute("""
#                 INSERT INTO raw_validated_data
#                 (title, description, bfc_message, la, ld, nf, ns, ent,
#                  nrev, rtime, ndev, age, aexp, asawr, revd, bugcount, fixcount)
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 RETURNING id
#             """, tuple(r[col] for col in REQUIRED_COLUMNS))
#             raw_ids.append(cur.fetchone()["id"])
#         conn.commit()
#         msg = f"Inserted {len(raw_ids)} rows into raw_validated_data"
#         logger.info(msg)
#         debug_messages.append(msg)

#         # -------------------- FEATURE PROCESSING --------------------
#         df["clean_text"] = df["title"] + " " + df["description"] + " " + df["BFC_message"]
#         ft_vectors = np.vstack(df["clean_text"].apply(fasttext_vector).values)

#         numeric_features = df[NUMERIC_COLUMNS_PLUS_BOOLEAN].values

#         debug_messages.append(f"FastText vector shape: {ft_vectors.shape}")
#         debug_messages.append(f"Numeric + boolean features shape: {numeric_features.shape}")

#         X = np.hstack([ft_vectors, numeric_features])
#         debug_messages.append(f"Combined feature matrix X shape: {X.shape}")

#         if X.shape[1] != expected_features:
#             raise ValueError(f"Feature mismatch! X has {X.shape[1]} features, but SVM expects {expected_features}")

#         # Predict
#         predictions = svm_model.predict(X)
#         df["predicted_label"] = predictions.astype(int)
#         df["predicted_type"] = df["predicted_label"].map(LABEL_MAP)

#         return {
#             "status": "success",
#             "rows_inserted": len(df),
#            "predictions": df[["title", "predicted_label", "predicted_type"]].to_dict(orient="records"),

#             "debug": debug_messages
#         }

#     except Exception:
#         error_trace = traceback.format_exc()
#         logger.error(error_trace)
#         debug_messages.append(error_trace)
#         raise HTTPException(status_code=500, detail={"message": "Internal server error", "debug": debug_messages})

#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()




# import logging
# from fastapi import FastAPI, UploadFile, File, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# import pandas as pd
# from io import StringIO
# from database import get_connection
# import pickle
# from pathlib import Path
# from gensim.models import FastText
# import numpy as np
# import traceback

# # -------------------- LOGGER --------------------
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # -------------------- APP --------------------
# app = FastAPI()

# # -------------------- CORS CONFIG --------------------
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "http://localhost:3000",
#         "http://127.0.0.1:3000"
#     ],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # -------------------- CONFIG --------------------
# REQUIRED_COLUMNS = [
#     "title", "description", "BFC_message",
#     "la", "ld", "nf", "ns", "ent",
#     "nrev", "rtime", "ndev", "age", "aexp", "asawr",
#     "revd", "bugcount", "fixcount"
# ]

# NUMERIC_COLUMNS = [
#     "la", "ld", "nf", "ns", "ent",
#     "nrev", "rtime", "ndev", "age", "aexp", "asawr",
#     "bugcount", "fixcount"
# ]

# BOOLEAN_COLUMNS = ["revd"]
# NUMERIC_COLUMNS_PLUS_BOOLEAN = NUMERIC_COLUMNS + BOOLEAN_COLUMNS
# LABEL_MAP = {0: "Extrinsic Bug", 1: "Intrinsic Bug"}

# # -------------------- LOAD MODELS --------------------
# SVM_MODEL_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\FastText_SVM.pkl")
# FASTTEXT_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\fasttext.model")

# with open(SVM_MODEL_PATH, "rb") as f:
#     svm_model = pickle.load(f)
# logger.info("✅ SVM model loaded successfully")

# ft_model = FastText.load(str(FASTTEXT_PATH))
# logger.info("✅ FastText model loaded successfully")

# expected_features = ft_model.vector_size + len(NUMERIC_COLUMNS_PLUS_BOOLEAN)
# logger.info(f"Expected features for SVM: {expected_features}")

# def fasttext_vector(text: str) -> np.ndarray:
#     words = str(text).split()
#     vectors = [ft_model.wv[word] for word in words if word in ft_model.wv]
#     return np.mean(vectors, axis=0) if vectors else np.zeros(ft_model.vector_size)

# # -------------------- API --------------------
# @app.post("/api/predict")
# def upload_csv(file: UploadFile = File(...)):
#     conn, cur = None, None
#     debug_messages = []
#     try:
#         # Read CSV
#         content = file.file.read().decode("utf-8-sig")
#         df = pd.read_csv(StringIO(content))
#         msg = f"CSV loaded with {len(df)} rows"
#         logger.info(msg)
#         debug_messages.append(msg)

#         # Validate columns
#         missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
#         if missing:
#             raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

#         # Reject NULLs
#         if df[REQUIRED_COLUMNS].isnull().any().any():
#             raise HTTPException(status_code=422, detail="CSV contains NULL values")

#         # Numeric conversion
#         for col in NUMERIC_COLUMNS:
#             df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

#         # Boolean conversion
#         for col in BOOLEAN_COLUMNS:
#             df[col] = df[col].apply(lambda x: 1 if str(x).upper() == "TRUE" else 0)

#         # -------------------- INSERT RAW DATA --------------------
#         conn = get_connection()
#         cur = conn.cursor()
#         raw_ids = []

#         for _, r in df.iterrows():
#             cur.execute("""
#                 INSERT INTO raw_validated_data
#                 (title, description, bfc_message, la, ld, nf, ns, ent,
#                  nrev, rtime, ndev, age, aexp, asawr, revd, bugcount, fixcount)
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 RETURNING id
#             """, tuple(r[col] for col in REQUIRED_COLUMNS))
#             raw_ids.append(cur.fetchone()["id"])
#         conn.commit()
#         msg = f"Inserted {len(raw_ids)} rows into raw_validated_data"
#         logger.info(msg)
#         debug_messages.append(msg)

#         # -------------------- FEATURE PROCESSING --------------------
#         df["clean_text"] = df["title"] + " " + df["description"] + " " + df["BFC_message"]
#         ft_vectors = np.vstack(df["clean_text"].apply(fasttext_vector).values)
#         numeric_features = df[NUMERIC_COLUMNS_PLUS_BOOLEAN].values
#         X = np.hstack([ft_vectors, numeric_features])

#         if X.shape[1] != expected_features:
#             raise ValueError(f"Feature mismatch! X has {X.shape[1]} features, expected {expected_features}")

#         # -------------------- PREDICTION --------------------
#         predictions = svm_model.predict(X)
#         df["predicted_label"] = predictions.astype(int)
#         df["predicted_type"] = df["predicted_label"].map(LABEL_MAP)

#         # -------------------- SUMMARY --------------------
#         total_bugs = len(df)
#         intrinsic_count = int((df["predicted_type"] == "Intrinsic Bug").sum())
#         extrinsic_count = int((df["predicted_type"] == "Extrinsic Bug").sum())

#         return {
#             "status": "success",
#             "rows_inserted": len(df),
#             "predictions": df[["title", "predicted_label", "predicted_type"]].to_dict(orient="records"),
#             "summary": {
#                 "total_bugs": total_bugs,
#                 "intrinsic": intrinsic_count,
#                 "extrinsic": extrinsic_count
#             },
#             "debug": debug_messages
#         }

#     except Exception:
#         error_trace = traceback.format_exc()
#         logger.error(error_trace)
#         debug_messages.append(error_trace)
#         raise HTTPException(status_code=500, detail={"message": "Internal server error", "debug": debug_messages})

#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()


import logging
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import StringIO
from database import get_connection
import pickle
from pathlib import Path
from gensim.models import FastText
import numpy as np
import traceback

# -------------------- LOGGER --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- APP --------------------
app = FastAPI()

# -------------------- CORS CONFIG --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- CONFIG --------------------
REQUIRED_COLUMNS = [
    "title", "description", "BFC_message",
    "la", "ld", "nf", "ns", "ent",
    "nrev", "rtime", "ndev", "age", "aexp", "asawr",
    "revd", "bugcount", "fixcount"
]

NUMERIC_COLUMNS = [
    "la", "ld", "nf", "ns", "ent",
    "nrev", "rtime", "ndev", "age", "aexp", "asawr",
    "bugcount", "fixcount"
]

BOOLEAN_COLUMNS = ["revd"]
NUMERIC_COLUMNS_PLUS_BOOLEAN = NUMERIC_COLUMNS + BOOLEAN_COLUMNS
LABEL_MAP = {0: "Extrinsic Bug", 1: "Intrinsic Bug"}

# -------------------- LOAD MODELS --------------------
SVM_MODEL_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\FastText_SVM.pkl")
FASTTEXT_PATH = Path(r"C:\Users\Sahana\Documents\sai-bug-backend\models\fasttext.model")

with open(SVM_MODEL_PATH, "rb") as f:
    svm_model = pickle.load(f)
logger.info("✅ SVM model loaded successfully")

ft_model = FastText.load(str(FASTTEXT_PATH))
logger.info("✅ FastText model loaded successfully")

expected_features = ft_model.vector_size + len(NUMERIC_COLUMNS_PLUS_BOOLEAN)
logger.info(f"Expected features for SVM: {expected_features}")

def fasttext_vector(text: str) -> np.ndarray:
    words = str(text).split()
    vectors = [ft_model.wv[word] for word in words if word in ft_model.wv]
    return np.mean(vectors, axis=0) if vectors else np.zeros(ft_model.vector_size)

# -------------------- API --------------------
@app.post("/api/predict")
def upload_csv(file: UploadFile = File(...)):
    conn, cur = None, None
    debug_messages = []  # Collect debug logs for frontend
    try:
        # Read CSV
        content = file.file.read().decode("utf-8-sig")
        df = pd.read_csv(StringIO(content))
        debug_messages.append(f"CSV loaded with {len(df)} rows")
        logger.info(f"CSV loaded with {len(df)} rows")

        # Validate columns
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

        # Reject NULLs
        if df[REQUIRED_COLUMNS].isnull().any().any():
            raise HTTPException(status_code=422, detail="CSV contains NULL values")

        # Numeric conversion
        for col in NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Boolean conversion
        for col in BOOLEAN_COLUMNS:
            df[col] = df[col].apply(lambda x: 1 if str(x).upper() == "TRUE" else 0)

        # -------------------- INSERT RAW DATA --------------------
        conn = get_connection()
        cur = conn.cursor()
        raw_ids = []

        for _, r in df.iterrows():
            cur.execute("""
                INSERT INTO raw_validated_data
                (title, description, bfc_message, la, ld, nf, ns, ent,
                 nrev, rtime, ndev, age, aexp, asawr, revd, bugcount, fixcount)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, tuple(r[col] for col in REQUIRED_COLUMNS))
            raw_ids.append(cur.fetchone()["id"])
        conn.commit()
        debug_messages.append(f"Inserted {len(raw_ids)} rows into raw_validated_data")
        logger.info(f"Inserted {len(raw_ids)} rows into raw_validated_data")

        # -------------------- FEATURE PROCESSING --------------------
        df["clean_text"] = df["title"] + " " + df["description"] + " " + df["BFC_message"]
        ft_vectors = np.vstack(df["clean_text"].apply(fasttext_vector).values)
        numeric_features = df[NUMERIC_COLUMNS_PLUS_BOOLEAN].values
        debug_messages.append(f"FastText vector shape: {ft_vectors.shape}")
        debug_messages.append(f"Numeric + boolean features shape: {numeric_features.shape}")

        X = np.hstack([ft_vectors, numeric_features])
        debug_messages.append(f"Combined feature matrix X shape: {X.shape}")

        if X.shape[1] != expected_features:
            raise ValueError(f"Feature mismatch! X has {X.shape[1]} features, but SVM expects {expected_features}")

        # -------------------- PREDICTION --------------------
        predictions = svm_model.predict(X)
        df["predicted_label"] = predictions.astype(int)
        df["predicted_type"] = df["predicted_label"].map(LABEL_MAP)

        # -------------------- INSERT PREDICTIONS --------------------
        for idx, row in df.iterrows():
            cur.execute("""
                INSERT INTO predictions (raw_id, predicted_label, predicted_type)
                VALUES (%s, %s, %s)
            """, (raw_ids[idx], int(row.predicted_label), row.predicted_type))
        conn.commit()
        debug_messages.append(f"Inserted {len(df)} rows into predictions table")
        logger.info(f"Inserted {len(df)} rows into predictions table")

        # -------------------- RETURN --------------------
        return {
            "status": "success",
            "rows_inserted": len(df),
            "predictions": df[["title", "predicted_label", "predicted_type"]].to_dict(orient="records"),
            "debug": debug_messages
        }

    except Exception:
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        debug_messages.append(error_trace)
        raise HTTPException(status_code=500, detail={"message": "Internal server error", "debug": debug_messages})

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
