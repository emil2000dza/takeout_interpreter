"""
topic_classification.py

Classifies Chrome history entries into topics using TOPIC_ASSIGNMENT_PROMPT in batch mode.
- Before 10k entries: LLM is used in batches of URLs
- After 10k entries: A TF-IDF classifier trained on (title, topics) is used
- Each classification result is immediately written into Snowflake
"""

import os
import json
import time
from typing import Dict, List
from tqdm import tqdm
from sqlalchemy import text
from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.pipeline import Pipeline
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression

from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import BATCH_TOPIC_ASSIGNMENT_PROMPT
from src.topic_modeling.utils import extract_json

# ---------- CONFIG ----------
load_dotenv()
DOOMSCROLLING_URLS = os.getenv("DOOMSCROLLING_URLS", "")
DOOMSCROLLING_LIST = [u.strip() for u in DOOMSCROLLING_URLS.split(",") if u.strip()]

NEW_CLASSIFICATION_TABLE = "CLASSIFICATION_HISTORY_TABLE"
REFINED_TOPICS_TABLE = "REFINED_TOPICS"
BATCH_SIZE = 20
LLM_LIMIT = 10_000

client_snowflake = SnowflakeORM()

# ---------- HELPERS ----------
def fetch_refined_topics() -> str:
    """Fetch refined topics (name + description only) from Snowflake."""
    with client_snowflake.session_scope() as session:
        rows = session.execute(text(f"""
            SELECT OBJECT_AGG(topic_name, OBJECT_CONSTRUCT(
                'description', description
            ))
            FROM {REFINED_TOPICS_TABLE}
        """)).fetchone()
        return json.dumps(rows[0]) if rows and rows[0] else "{}"


def write_entry(entry: Dict) -> None:
    """Insert a single classified entry into Snowflake."""
    with client_snowflake.session_scope() as session:
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {NEW_CLASSIFICATION_TABLE} (
                title STRING,
                url STRING,
                topics ARRAY
            )
        """))
        session.execute(
            text(f"""
                INSERT INTO {NEW_CLASSIFICATION_TABLE} (title, url, topics)
                SELECT $1, $2, PARSE_JSON($3)
                FROM VALUES (:title, :url, :topics);
            """),
            {
                "title": entry["title"],
                "url": entry["url"],
                "topics": json.dumps(entry.get("topics", []))
            }
        )


def classify_batch(batch: List[Dict], topics_json: str) -> Dict[str, List[str]]:
    """
    Call LLM on a batch of entries, return mapping url -> topics list.
    Doomscrolling is always merged after LLM classification so both can coexist.
    """
    mapping: Dict[str, List[str]] = {}

    # Format prompt with all non-doomscrolling entries
    urls_for_llm = [{"title": e["title"], "url": e["url"]} for e in batch]
    if urls_for_llm:
        prompt = BATCH_TOPIC_ASSIGNMENT_PROMPT.format(
            urls=json.dumps(urls_for_llm, indent=2),
            topics_json=topics_json
        )
        response = call_llm(prompt)

        try:
            llm_output = extract_json(response)
            for url, obj in llm_output.items():
                mapping[url] = obj.get("classes", [])
        except Exception as e:
            print(f"⚠ Failed batch classification: {e}")

    for e in batch:
        if any(bad in e["url"] for bad in DOOMSCROLLING_LIST):
            if e["url"] not in mapping:
                mapping[e["url"]] = []
            if "Doomscrolling" not in mapping[e["url"]]:
                mapping[e["url"]].append("Doomscrolling")

    return mapping

# ---------- MAIN ----------
if __name__ == "__main__":
    topics_json = fetch_refined_topics()

    # Fetch all urls
    with client_snowflake.session_scope() as session:
        rows = session.query(
            ChromeHistory.title,
            ChromeHistory.url
        ).distinct().all()

    # Count already stored
    with client_snowflake.session_scope() as session:
        # Ensure table exists
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {NEW_CLASSIFICATION_TABLE} (
                title STRING,
                url STRING,
                topics ARRAY
            )
        """))

        result = session.execute(text(f"SELECT COUNT(*) FROM {NEW_CLASSIFICATION_TABLE}")).fetchone()
        done_count = result[0] if result and result[0] is not None else 0
    print(f"Already classified: {done_count}")

    titles, urls, labels = [], [], []
    for i in tqdm(range(0, len(rows), BATCH_SIZE)):
        batch_rows = rows[i:i+BATCH_SIZE]
        batch = [{"title": r.title, "url": r.url} for r in batch_rows]

        if done_count < LLM_LIMIT:
            mapping = classify_batch(batch, topics_json)
            for e in batch:
                url = e["url"]
                topics = mapping.get(url, [])
                write_entry({"title": e["title"], "url": url, "topics": topics})

                if topics:
                    titles.append(e["title"])
                    urls.append(url)
                    labels.append(topics)

            done_count += len(batch)
        else:
            # ---- Train classifier if not yet trained ----
            if not titles:
                print("⚠ No training data collected for classifier")
                break

            print("🔨 Training local classifier...")
            mlb = MultiLabelBinarizer()
            Y = mlb.fit_transform(labels) # recréer labels et titles
            clf = Pipeline([
                ("tfidf", TfidfVectorizer(max_features=20_000, ngram_range=(1, 2))),
                ("clf", OneVsRestClassifier(LogisticRegression(max_iter=1000)))
            ])
            clf.fit(titles, Y)
            print("✅ Classifier trained, switching to local predictions")

            # ---- Infer remaining entries ----
            for j in tqdm(range(i, len(rows))):
                r = rows[j]
                pred = clf.predict([r.title])
                topics = mlb.inverse_transform(pred)[0]
                write_entry({"title": r.title, "url": r.url, "topics": list(topics)})

            break
