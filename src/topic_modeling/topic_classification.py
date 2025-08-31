"""
topic_classification.py

Reusable and optimized module for classifying Chrome history entries into topics.

Optimizations:
- Stream Chrome history rows in chunks (no full-table load).
- Fetch only classified URLs (lighter memory use).
- Batch inserts into Snowflake (reduce round-trips).
- Optional classifier persistence via joblib.
"""

import logging
import os
import json
from typing import Dict, List, Tuple, Any

import joblib
from tqdm import tqdm
from sqlalchemy import text
from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.pipeline import Pipeline
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression

from src.db.snowflake_client import SnowflakeORM
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import BATCH_TOPIC_ASSIGNMENT_PROMPT
from src.topic_modeling.utils import extract_json, fetch_topics
from src.topic_modeling.topic_refinment import REFINED_TOPICS_TABLE

logging.basicConfig(
    level=logging.INFO,  # or INFO if preferred
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)


# ---------- CONFIG ----------
load_dotenv()

SCROLLING_URLS = os.getenv("SCROLLING_URLS", "")
SCROLLING_LIST = [u.strip() for u in SCROLLING_URLS.split(",") if u.strip()]

NEW_CLASSIFICATION_TABLE = "CLASSIFICATION_HISTORY_TABLE"
MODEL_PATH = "topic_classifier.joblib"

client_snowflake = SnowflakeORM()

# ---------- HELPERS ----------
def ensure_classification_table_exists(domain : str) -> None:
    """Ensure classification table exists in Snowflake."""
    classification_table_name = domain.rsplit('.', 1)[0].replace('.', '_').upper() + '_' + NEW_CLASSIFICATION_TABLE
    
    with client_snowflake.session_scope() as session:
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {classification_table_name} (
                title STRING,
                url STRING,
                topics ARRAY
            )
        """))

def write_entries(entries: List[Dict], domain: str) -> None:
    """Insert multiple classified entries into Snowflake in bulk with PARSE_JSON for ARRAY columns."""
    classification_table = f"{domain.rsplit('.', 1)[0].replace('.', '_').upper()}_{NEW_CLASSIFICATION_TABLE}"

    if not entries:
        return

    sql = text(f"""
        INSERT INTO {classification_table} (title, url, topics)
        SELECT $1, $2, PARSE_JSON($3)
        FROM VALUES (:title, :url, :topics)
    """)

    params = [
        {
            "title": e["title"],
            "url": e["url"],
            "topics": json.dumps(e.get("topics", []))  # JSON string gets parsed
        }
        for e in entries
    ]

    with client_snowflake.session_scope() as session:
        session.execute(sql, params)

def classify_batch(batch: List[Dict], topics_json: str, domain : str) -> Dict[str, List[str]]:
    """Call LLM on a batch of entries and return mapping url -> topics list."""
    mapping: Dict[str, List[str]] = {}

    urls_for_llm = [{"title": e["title"], "url": e["url"]} for e in batch]
    if urls_for_llm:
        prompt = BATCH_TOPIC_ASSIGNMENT_PROMPT.format(
            urls=json.dumps(urls_for_llm, indent=2),
            topics_json=topics_json,
            domain=domain
        )
        response = call_llm(prompt)

        try:
            llm_output = extract_json(response)
            for url, obj in llm_output.items():
                mapping[url] = obj.get("classes", ["None"])
        except Exception as exc:
            logger.warning(f"⚠ Failed batch classification: {exc}")

    for entry in batch:
        if any(bad in entry["url"] for bad in SCROLLING_LIST):
            mapping.setdefault(entry["url"], [])
            if "Scrolling" not in mapping[entry["url"]]:
                mapping[entry["url"]].append("Scrolling")

    return mapping


def train_local_classifier(
    titles: List[str], labels: List[List[str]]
) -> Tuple[Pipeline, MultiLabelBinarizer]:
    """Train and return a TF-IDF based classifier with label binarizer."""
    mlb = MultiLabelBinarizer()
    y = mlb.fit_transform(labels)

    clf = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=20_000, ngram_range=(1, 2))),
        ("clf", OneVsRestClassifier(LogisticRegression(max_iter=1000))),
    ])
    clf.fit(titles, y)

    joblib.dump((clf, mlb), MODEL_PATH)
    logger.info(f"✅ Classifier trained and saved to {MODEL_PATH}")
    return clf, mlb


def load_local_classifier() -> Tuple[Pipeline, MultiLabelBinarizer]:
    """Load persisted classifier if available."""
    if os.path.exists(MODEL_PATH):
        logger.info(f"📂 Loading classifier from {MODEL_PATH}")
        return joblib.load(MODEL_PATH)
    return None, None


def get_existing_urls(domain: str) -> Tuple[set, int]:
    """Fetch already classified URLs and count."""
    classification_table = domain.rsplit('.', 1)[0].replace('.', '_').upper() + '_' + NEW_CLASSIFICATION_TABLE
    with client_snowflake.session_scope() as session:
        result = session.execute(
            text(f"SELECT COUNT(DISTINCT url) FROM {classification_table}")
        ).fetchone()
        done_count = result[0] if result and result[0] is not None else 0

        rows = session.execute(
            text(f"SELECT url FROM {classification_table}")
        ).fetchall()

    return {row[0] for row in rows}, done_count

def classify_entries(history_rows: List[Any], 
                     domain : str,
                     BATCH_SIZE: int = 20,
                     INSERT_BATCH_SIZE : int = 100,
                     LLM_LIMIT : int = 10_000) -> None:
    """
    Classify a list of history rows given refined topics.

    Parameters
    ----------
    history_rows : list
        List of ChromeHistory-like objects (must have `.title` and `.url`).
    refined_topics : dict
        Mapping of topics used for classification (from run_topic_refinement).
    """
    refined_table_name = domain.rsplit('.', 1)[0].replace('.', '_').upper() + '_' + REFINED_TOPICS_TABLE
    topics_json = json.dumps(
        [{'name': k, 'description': v} 
         for k, v in fetch_topics(client_snowflake, refined_table_name)]
    )

    ensure_classification_table_exists(domain)
    classified_urls, done_count = get_existing_urls(domain)

    history_rows_distinct = [dict(t) for t in {frozenset(d.items()) for d in history_rows}]
    rows = [row for row in history_rows_distinct if row['url'] not in classified_urls]
    logger.info(f"Skipping {len(classified_urls)} already classified URLs.")

    titles, labels = [], []
    clf, mlb = load_local_classifier()

    buffer: List[Dict] = []

    for i in tqdm(range(0, len(rows), BATCH_SIZE)):
        batch_rows = rows[i:i + BATCH_SIZE]

        if done_count < LLM_LIMIT:
            mapping = classify_batch(batch_rows, topics_json, domain)
            for entry in batch_rows:
                url = entry["url"]
                topics = mapping.get(url, [])
                buffer.append({"title": entry["title"], "url": url, "topics": topics})

                if topics:
                    titles.append(entry["title"])
                    labels.append(topics)

            done_count += len(batch_rows)

        else:
            if clf is None or mlb is None:
                if not titles:
                    logger.warning("⚠ No training data available for classifier")
                    break
                clf, mlb = train_local_classifier(titles, labels)

            for r in batch_rows:
                topics = ["None"]
                if r['title']:
                    pred = clf.predict([r['title']])
                    topics = list(mlb.inverse_transform(pred)[0])
                buffer.append({"title": r['title'], "url": r['url'], "topics": topics})

        if len(buffer) >= INSERT_BATCH_SIZE:
            write_entries(buffer, domain)
            buffer.clear()

    if buffer:
        write_entries(buffer, domain)