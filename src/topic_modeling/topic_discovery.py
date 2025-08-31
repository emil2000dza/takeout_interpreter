"""
topic_discovery.py

Runs topic discovery in batches over Chrome history using TOPIC_ASSIGNMENT_PROMPT,
parses the resulting JSON, and stores unique topics in a Snowflake table.
"""

import json
import logging
import random
from typing import List, Dict
from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory
from src.topic_modeling.gemini import call_llm 
from src.topic_modeling.utils import extract_json, write_topics_to_snowflake, has_existing_topics
from src.topic_modeling.prompts import TOPIC_DISCOVERY_PROMPT

logging.basicConfig(
    level=logging.INFO,  # or INFO if preferred
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

# ---------- CONFIG ----------
NEW_TOPICS_TABLE = "DISCOVERED_TOPICS_IN_HISTORY"
client_snowflake = SnowflakeORM()


# ---------- FUNCTIONS ----------
def fetch_history(domain : str) -> List[Dict]:
    """Fetch Chrome history from Snowflake."""
    with client_snowflake.session_scope() as session:
        query = session.query(
            ChromeHistory.title,
            ChromeHistory.url,
        ).filter(ChromeHistory.domain == domain)

        rows = query.all()
        
        history = [
            {"title": r.title, "url": r.url}
            for r in rows
        ]
        random.shuffle(history)
        return history


def chunk_list(data: List, batch_size: int = 1000) -> List[List]:
    """Split list into chunks of given size."""
    return [data[i:i + batch_size] for i in range(0, len(data), batch_size)]


def format_batch_for_prompt(batch: List[Dict]) -> str:
    """Format a batch of history rows into a JSON string for the prompt."""
    cleaned = [
        {
            "title": row["title"],
            "url": row["url"]
        }
        for row in batch
        if row.get("title") and row.get("url")
    ]
    return json.dumps(cleaned, ensure_ascii=False)

def run_topic_discovery(rows: List[Dict], domain : str, batch_size: int=1_000):
    """Run topic discovery across all batches and merge results."""
    if has_existing_topics(client_snowflake, 
                           domain.rsplit('.', 1)[0].replace('.','_').upper() + '_' + NEW_TOPICS_TABLE,
                           min_count = len(rows) // batch_size):
        return

    seen_topics = set()

    batches = chunk_list(rows, batch_size)
    logger.info(f"🔍 Processing {len(batches)} batches...")

    for i, batch in enumerate(batches, start=1):
        prompt = TOPIC_DISCOVERY_PROMPT.format(
            history_sample=format_batch_for_prompt(batch),
            domain=domain
        )
        response = call_llm(prompt)

        try:
            topics_dict = extract_json(response)
            new_topics_dict = {t: d for t, d in topics_dict.items() if t not in seen_topics}
        except json.JSONDecodeError:
            logger.warning(f"⚠ Batch {i} returned invalid JSON. Skipping.")
            logger.info(response)
            continue

        if new_topics_dict:
            write_topics_to_snowflake(client_snowflake, new_topics_dict, domain.rsplit('.', 1)[0].replace('.','_').upper() + '_' + NEW_TOPICS_TABLE)
            seen_topics.update(new_topics_dict.keys())
            logger.info(f"✅ Batch {i} processed. Topics written: {len(new_topics_dict)}")
        else:
            logger.warning(f"ℹ Batch {i} produced no new topics.")

    logger.info(f"🎯 Total unique topics stored: {len(seen_topics)}")