import os 

from dotenv import load_dotenv
from src.topic_modeling.topic_discovery import fetch_history, run_topic_discovery
from src.topic_modeling.topic_refinment import fetch_discovered_topics, run_topic_refinement

load_dotenv()

DOMAIN_TO_MODEL = os.getenv("DOMAIN_NAMES_FOR_TOPIC_MODELING", "")

for domain in DOMAIN_TO_MODEL:
    history_rows = fetch_history(domain)
    run_topic_discovery(history_rows)

    topics = fetch_discovered_topics(domain)
    refined_topics = run_topic_refinement(topics, domain)

    
