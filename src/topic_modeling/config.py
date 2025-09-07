import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    # table suffixes
    DISCOVERED_TOPICS_SUFFIX: str = os.getenv("DISCOVERED_TOPICS_SUFFIX", "DISCOVERED_TOPICS_IN_HISTORY")
    REFINED_TOPICS_SUFFIX: str = os.getenv("REFINED_TOPICS_SUFFIX", "REFINED_TOPICS")
    CLASSIFICATION_SUFFIX: str = os.getenv("CLASSIFICATION_SUFFIX", "CLASSIFICATION_HISTORY_TABLE")

    # batching
    DISCOVERY_BATCH: int = int(os.getenv("DISCOVERY_BATCH", "1000"))
    CLASSIFY_BATCH: int = int(os.getenv("CLASSIFY_BATCH", "20"))
    INSERT_BATCH: int = int(os.getenv("INSERT_BATCH", "10")) # interesting to modify if difficulties with api timeout
    LLM_LIMIT: int = int(os.getenv("LLM_LIMIT", "10000"))

    # model persistence
    MODEL_PATH: str = os.getenv("MODEL_PATH", "topic_classifier.joblib")

    # miscellaneous
    SCROLLING_URLS: str = os.getenv("SCROLLING_URLS", "")

    @property
    def scrolling_list(self) -> List[str]:
        return [u.strip() for u in self.SCROLLING_URLS.split(",") if u.strip()]
