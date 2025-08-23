"""
topic_discovery.py

Runs topic discovery in batches over Chrome history using TOPIC_ASSIGNMENT_PROMPT,
parses the resulting JSON, and stores unique topics in a Snowflake table.
"""

import json
import random
from typing import List, Dict
from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory
from src.topic_modeling.gemini import call_llm 
from src.topic_modeling.utils import extract_json, write_topics_to_snowflake
from src.topic_modeling.prompts import TOPIC_DISCOVERY_PROMPT

# ---------- CONFIG ----------
BATCH_SIZE = 1000
NEW_TOPICS_TABLE = "DISCOVERED_TOPICS_IN_HISTORY"
client_snowflake = SnowflakeORM()


# ---------- FUNCTIONS ----------
def fetch_history(limit: int = None) -> List[Dict]:
    """Fetch Chrome history from Snowflake."""
    with client_snowflake.session_scope() as session:
        query = session.query(
            ChromeHistory.title,
            ChromeHistory.url,
            ChromeHistory.domain
        )
        if limit:
            query = query.limit(limit)
        rows = query.all()
        
        history = [
            {"title": r.title, "url": r.url, "domain": r.domain}
            for r in rows
        ]
        random.shuffle(history)
        return history


def chunk_list(data: List, size: int) -> List[List]:
    """Split list into chunks of given size."""
    return [data[i:i + size] for i in range(0, len(data), size)]


def format_batch_for_prompt(batch: List[Dict]) -> str:
    """Format a batch of history rows into a JSON string for the prompt."""
    cleaned = [
        {
            "title": row["title"],
            "url": row["url"],
            "domain": row.get("domain", "")
        }
        for row in batch
        if row.get("title") and row.get("url")
    ]
    return json.dumps(cleaned, ensure_ascii=False)

def run_topic_discovery(rows: List[Dict]):
    """Run topic discovery across all batches and merge results."""
    seen_topics = set()

    batches = chunk_list(rows, BATCH_SIZE)
    print(f"🔍 Processing {len(batches)} batches...")

    for i, batch in enumerate(batches, start=1):
        prompt = TOPIC_DISCOVERY_PROMPT.format(
            history_sample=format_batch_for_prompt(batch)
        )
        response = call_llm(prompt)

        try:
            topics_dict = extract_json(response)
        except json.JSONDecodeError:
            print(f"⚠ Batch {i} returned invalid JSON. Skipping.")
            print(response)
            continue

        # Filter only new topics
        new_topics_dict = {t: d for t, d in topics_dict.items() if t not in seen_topics}

        if new_topics_dict:
            write_topics_to_snowflake(client_snowflake, new_topics_dict, NEW_TOPICS_TABLE)
            seen_topics.update(new_topics_dict.keys())
            print(f"✅ Batch {i} processed. Topics written: {len(new_topics_dict)}")
        else:
            print(f"ℹ Batch {i} produced no new topics.")

    print(f"🎯 Total unique topics stored: {len(seen_topics)}")


# ---------- MAIN ----------
if __name__ == "__main__":
    history_rows = fetch_history()
    run_topic_discovery(history_rows)