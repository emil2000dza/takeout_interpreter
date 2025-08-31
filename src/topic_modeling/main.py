import logging
import os 

from dotenv import load_dotenv
from src.topic_modeling.topic_discovery import fetch_history, run_topic_discovery
from src.topic_modeling.topic_refinment import run_topic_refinement
from src.topic_modeling.topic_classification import classify_entries

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
DOMAIN_TO_MODEL = os.getenv("DOMAIN_NAMES_FOR_TOPIC_MODELING", "")
DOMAIN_TO_MODEL_LIST = [u.strip() for u in DOMAIN_TO_MODEL.split(",") if u.strip()]

for domain in DOMAIN_TO_MODEL_LIST:
    logger.info(f'⚙⚙ PROCESSING {domain.upper()}')

    if not domain.rsplit('.', 1)[0]:
        raise(f"Invalid URL entered: '{domain.rsplit('.', 1)[0].replace('.', '_')}'")

    history_rows = fetch_history(domain)

    run_topic_discovery(history_rows, domain)
    run_topic_refinement(domain)
    classify_entries(history_rows, domain)

    
