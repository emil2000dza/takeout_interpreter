"""
topic_refinement.py

Runs topic refinement on discovered topics using TOPIC_REFINEMENT_PROMPT,
parses the resulting JSON, and writes refined topics into Snowflake.
"""
from sqlalchemy import text

from src.db.snowflake_client import SnowflakeORM
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import TOPIC_REFINMENT_PROMPT
from src.topic_modeling.utils import extract_json, format_topics, write_topics_to_snowflake

# ---------- CONFIG ----------
REFINED_TOPICS_TABLE = "REFINED_TOPICS"
client_snowflake = SnowflakeORM()


# ---------- FUNCTIONS ----------
def fetch_discovered_topics(domain : str):
    """Fetch discovered topics from Snowflake."""
    discovered_topics_table_name = domain.upper() + REFINED_TOPICS_TABLE
    with client_snowflake.session_scope() as session:
        topics = session.execute(
            text(f"SELECT topic_name, topic_description, topics FROM {discovered_topics_table_name}")
        ).fetchall()
        return topics

def run_topic_refinement(topics, domain : str):
    """Run LLM refinement on discovered topics and return structured output."""
    prompt = TOPIC_REFINMENT_PROMPT.format(
        all_topics=format_topics(topics),
        domain=domain
    )
    response = call_llm(prompt)

    try:
        refined_topics = extract_json(response)
    except Exception:
        raise
    write_topics_to_snowflake(client_snowflake, refined_topics, domain.upper() + REFINED_TOPICS_TABLE)
    print(f"✅ {len(refined_topics)} refined topics written to Snowflake.")
