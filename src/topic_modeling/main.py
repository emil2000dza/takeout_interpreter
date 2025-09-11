import os
from typing import Optional

from dotenv import load_dotenv

from src.topic_modeling.pipeline import TopicModelingPipeline

load_dotenv()
DOMAIN_TO_MODEL = os.getenv("DOMAIN_NAMES_FOR_TOPIC_MODELING", "")
DOMAIN_TO_MODEL_LIST = [u.strip() for u in DOMAIN_TO_MODEL.split(",") if u.strip()]

def run_for_domain(domain: str, sample_limit: Optional[int] = None):
    """Example end-to-end runner keeping the API surface tiny."""
    pipe = TopicModelingPipeline(domain)
    pipe.discover_topics(sample_limit=sample_limit)
    pipe.refine_topics()

    # Stream history for classification to avoid high memory usage
    with pipe.repo as db:
        entries = db.fetch_history_by_domain(domain)
        pipe.classify(entries)

if __name__ == '__main__':
    for domain in DOMAIN_TO_MODEL_LIST:
        run_for_domain(domain)
