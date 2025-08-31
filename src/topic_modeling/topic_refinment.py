"""
topic_refinement.py

Runs topic refinement on discovered topics using TOPIC_REFINEMENT_PROMPT,
parses the resulting JSON, and writes refined topics into Snowflake.
"""

import logging

from sqlalchemy import text

from src.db.snowflake_client import SnowflakeORM
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import TOPIC_REFINMENT_PROMPT
from src.topic_modeling.utils import extract_json, format_topics, write_topics_to_snowflake, has_existing_topics, fetch_topics
from src.topic_modeling.topic_discovery import NEW_TOPICS_TABLE

logging.basicConfig(
    level=logging.INFO,  # or INFO if preferred
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

# ---------- CONFIG ----------
REFINED_TOPICS_TABLE = "REFINED_TOPICS"
client_snowflake = SnowflakeORM()


def run_topic_refinement(domain : str):
    """Run LLM refinement on discovered topics and return structured output."""
    refined_table_name = domain.rsplit('.', 1)[0].replace('.','_').upper() + '_' + REFINED_TOPICS_TABLE
    discovery_table_name = domain.rsplit('.', 1)[0].replace('.','_').upper() + '_' + NEW_TOPICS_TABLE
    
    if has_existing_topics(client_snowflake, 
                           refined_table_name,
                           min_count = 3):
        return fetch_topics(client_snowflake, refined_table_name)
    
    topics = fetch_topics(client_snowflake, discovery_table_name)
    prompt = TOPIC_REFINMENT_PROMPT.format(
        all_topics=format_topics(topics),
        domain=domain
    )
    response = call_llm(prompt)

    try:
        refined_topics = extract_json(response)
    except Exception:
        raise
    write_topics_to_snowflake(client_snowflake, refined_topics, refined_table_name)
    logger.info(f"✅ {len(refined_topics)} refined topics written to Snowflake.")
    return refined_topics
