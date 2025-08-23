import sys
import os

# add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from src.db.snowflake_client import SnowflakeORM
from src.db.tables import DiscoveredTopics

client_snowflake = SnowflakeORM()

with client_snowflake.session_scope() as session:
        query = session.query(
            DiscoveredTopics.topic_name,
            DiscoveredTopics.description,
        )
        all_topics = query.all()

from src.topic_modeling.gemini import call_llm 
from src.topic_modeling.prompts import TOPIC_REFINMENT_PROMPT
from src.topic_modeling.topic_discovery import extract_json
import json

def format_topics(topics):
    """Format a list of (title, description) into a single structured text block."""
    lines = []
    for i, (title, desc) in enumerate(topics, start=1):
        lines.append(f"Topic {i}:\n- Name: {title}\n- Description: {desc}\n")
    return "\n".join(lines)

prompt = TOPIC_REFINMENT_PROMPT.format(
    all_topics=format_topics(all_topics)
    )
response = call_llm(prompt)

try:
    topics_dict = extract_json(response)
    print(topics_dict)
except json.JSONDecodeError:
    print(response)
