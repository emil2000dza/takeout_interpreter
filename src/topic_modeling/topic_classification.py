"""
topic_classification.py

Classifies each Chrome history entry into 0–5 topics using TOPIC_CLASSIFICATION_PROMPT.
If an entry matches a "doomscrolling" URL, it is tagged accordingly.
Each classification result is immediately written into Snowflake,
so progress is not lost if the script stops midway.
"""

import os
import json
from tqdm import tqdm
from typing import List, Dict
from sqlalchemy import text
from dotenv import load_dotenv
import time

from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory, ClassifiedHistoryTopics
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import TOPIC_ASSIGNMENT_PROMPT
from src.topic_modeling.utils import extract_json

# ---------- CONFIG ----------
load_dotenv()
DOOMSCROLLING_URLS = os.getenv("DOOMSCROLLING_URLS", "")
DOOMSCROLLING_LIST = [u.strip() for u in DOOMSCROLLING_URLS.split(",") if u.strip()]

NEW_CLASSIFICATION_TABLE = "CLASSIFIED_HISTORY_TOPICS"
REFINED_TOPICS_TABLE = "REFINED_TOPICS"
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


def classify_entry(entry: Dict, topics_json: str) -> List[str]:
    """
    Classify a single browsing history entry into topics.
    - Always add "Doomscrolling" if the URL matches DOOMSCROLLING_LIST.
    - Still call the LLM to allow other topics to be assigned.
    """
    topics = []

    if any(bad_url in entry["url"] for bad_url in DOOMSCROLLING_LIST):
        topics.append("Doomscrolling")

    prompt = TOPIC_ASSIGNMENT_PROMPT.format(
        title=entry["title"],
        url=entry["url"],
        topics_json=topics_json
    )
    response = call_llm(prompt)

    try:
        llm_topics = extract_json(response)
        for t in llm_topics:
            if t not in topics:
                topics.append(t)
    except Exception as e:
        print(f"⚠ Failed to classify entry {entry['title']}: {e}")

    return topics


def write_single_entry_to_snowflake(entry: Dict) -> None:
    """Insert a single classified entry directly into Snowflake."""
    with client_snowflake.session_scope() as session:
        session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {NEW_CLASSIFICATION_TABLE} (
                id STRING,
                title STRING,
                url STRING,
                domain STRING,
                topics ARRAY
            )
        """))

        session.execute(
            text(f"""
                INSERT INTO {NEW_CLASSIFICATION_TABLE} (id, title, url, domain, topics)
                SELECT $1, $2, $3, $4, PARSE_JSON($5)
                FROM VALUES (:id, :title, :url, :domain, :topics);
            """),
            {
                "id": entry["id"],
                "title": entry["title"],
                "url": entry["url"],
                "domain": entry["domain"],
                "topics": json.dumps(entry.get("topics", []))
            }
        )


# ---------- MAIN ----------
if __name__ == "__main__":
    topics_json = fetch_refined_topics()

    with client_snowflake.session_scope() as session:
        rows = session.query(
            ChromeHistory.title,
            ChromeHistory.url,
            ChromeHistory.domain
        ).distinct().all()

        urls_already_predicted = session.query(ClassifiedHistoryTopics.url).distinct().all()

        rows_to_process = list(set([row for row in rows if row.url not in [url[0] for url in urls_already_predicted]]))
        print(len(rows_to_process))
        for r in tqdm(rows_to_process):
            entry = {"id": 404, "title": r.title, "url": r.url, "domain": r.domain}
            entry["topics"] = classify_entry(entry, topics_json)
            write_single_entry_to_snowflake(entry)
            time.sleep(1)
            print(f"✅ Stored classified entry {entry['id']}")
