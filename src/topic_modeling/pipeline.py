import json
import logging
import os
import random
from typing import (Dict, Iterable, Iterator, List, Optional, Sequence, Set,
                    Tuple)

import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer

from src.topic_modeling.config import AppConfig
from src.topic_modeling.data_models import HistoryEntry
from src.topic_modeling.db import SnowflakeRepository
from src.topic_modeling.gemini import call_llm
from src.topic_modeling.prompts import (BATCH_TOPIC_ASSIGNMENT_PROMPT,
                                        TOPIC_DISCOVERY_PROMPT,
                                        TOPIC_REFINMENT_PROMPT)
from src.topic_modeling.utils import (extract_json, fetch_topics,
                                      format_topics, has_existing_topics,
                                      table_name, write_topics_to_snowflake)

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("topic_pipeline")


class TopicModelingPipeline:
    def __init__(self, domain: str, cfg: Optional[AppConfig] = None):
        self.domain = domain
        self.cfg = cfg or AppConfig()
        self.repo = SnowflakeRepository()
        # Precompute table names
        self.discovered_table = table_name(domain, self.cfg.DISCOVERED_TOPICS_SUFFIX)
        self.refined_table = table_name(domain, self.cfg.REFINED_TOPICS_SUFFIX)
        self.classification_table = table_name(domain, self.cfg.CLASSIFICATION_SUFFIX)

    # ---------- Discovery ----------
    def discover_topics(self, sample_limit: Optional[int] = None) -> None:
        with self.repo as db:
            # Optional sampling to keep LLM cost bounded
            entries = list(db.fetch_history_by_domain(self.domain, limit=sample_limit))
            random.shuffle(entries)

            if has_existing_topics(db._orm, self.discovered_table, min_count=max(1, len(entries) // self.cfg.DISCOVERY_BATCH)):
                log.info("Discovered topics already present; skipping discovery.")
                return

            batches = _chunk(entries, self.cfg.DISCOVERY_BATCH)
            log.info("🔍 Processing %d discovery batches…", len(batches))

            seen: Set[str] = set()
            for i, batch in enumerate(batches, start=1):
                prompt = TOPIC_DISCOVERY_PROMPT.format(
                    history_sample=json.dumps([e.__dict__ for e in batch], ensure_ascii=False),
                    domain=self.domain,
                )
                resp = call_llm(prompt)
                try:
                    topics = extract_json(resp)
                except Exception:
                    log.warning("⚠️ Invalid JSON in discovery batch %s; skipping.", i)
                    continue

                new_topics = {k: v for k, v in topics.items() if k not in seen}
                if not new_topics:
                    log.info("ℹ️ No new topics in batch %s", i)
                    continue

                write_topics_to_snowflake(db._orm, new_topics, self.discovered_table)
                seen.update(new_topics)
                log.info("✅ Batch %s written (%d new topics)", i, len(new_topics))

    # ---------- Refinement ----------
    def refine_topics(self) -> List[Tuple[str, str]]:
        with self.repo as db:
            if has_existing_topics(db._orm, self.refined_table, min_count=3):
                return fetch_topics(db._orm, self.refined_table)

            topics = fetch_topics(db._orm, self.discovered_table)
            prompt = TOPIC_REFINMENT_PROMPT.format(
                all_topics=format_topics(topics),
                domain=self.domain,
            )
            resp = call_llm(prompt)
            refined = extract_json(resp)
            write_topics_to_snowflake(db._orm, refined, self.refined_table)
            log.info("✅ %d refined topics written", len(refined))
            # Return normalized list[(name, description)] for reuse
            return [(k, v) for k, v in refined.items()]

    # ---------- Classification ----------
    def classify(self, entries: Iterable[HistoryEntry]) -> None:
        with self.repo as db:
            db.ensure_classification_table(self.classification_table)

            already = db.distinct_classified_urls(self.classification_table)
            done_count = db.count_classified_urls(self.classification_table)
            log.info("Skipping %d already-classified URLs.", len(already))

            topics_json = json.dumps(
                [
                    {"name": name, "description": desc}
                    for name, desc in fetch_topics(db._orm, self.refined_table)
                ]
            )

            # Local classifier cache
            titles: List[str] = []
            labels: List[List[str]] = []
            clf, mlb = _load_local_classifier(self.cfg.MODEL_PATH)

            buffer: List[Dict[str, object]] = []

            # Deduplicate + filter
            def _distinct(elems: Iterable[HistoryEntry]) -> Iterator[HistoryEntry]:
                seen_urls: Set[str] = set()
                for e in elems:
                    if e.url in seen_urls or e.url in already:
                        continue
                    seen_urls.add(e.url)
                    yield e

            for batch in _chunk_iter(_distinct(entries), self.cfg.CLASSIFY_BATCH):
                if done_count < self.cfg.LLM_LIMIT:
                    mapping = _classify_batch_llm(batch, topics_json, self.domain, self.cfg.scrolling_list)
                    for e in batch:
                        topics = mapping.get(e.url, [])
                        buffer.append({"title": e.title, "url": e.url, "topics": topics})
                        if topics:
                            titles.append(e.title)
                            labels.append(topics)
                    done_count += len(batch)
                else:
                    if clf is None or mlb is None:
                        if not titles:
                            log.warning("⚠️ No training data available for classifier; stopping.")
                            break
                        clf, mlb = _train_local_classifier(titles, labels, self.cfg.MODEL_PATH)
                    for e in batch:
                        topics = ["None"]
                        if e.title:
                            pred = clf.predict([e.title])
                            topics = list(mlb.inverse_transform(pred)[0])
                        buffer.append({"title": e.title, "url": e.url, "topics": topics})

                if len(buffer) >= self.cfg.INSERT_BATCH:
                    db.write_classifications(self.classification_table, buffer)
                    buffer.clear()

            if buffer:
                db.write_classifications(self.classification_table, buffer)


# ===================
# Helper functions
# ===================

def _chunk(data: Sequence, size: int) -> List[Sequence]:
    return [data[i : i + size] for i in range(0, len(data), size)]


def _chunk_iter(it: Iterable, size: int) -> Iterator[List]:
    buf: List = []
    for x in it:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def _classify_batch_llm(
    batch: Sequence[HistoryEntry], topics_json: str, domain: str, scrolling_list: List[str]
) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    payload = [{"title": e.title, "url": e.url} for e in batch]

    if payload:
        prompt = BATCH_TOPIC_ASSIGNMENT_PROMPT.format(
            urls=json.dumps(payload, indent=2), topics_json=topics_json, domain=domain
        )
        resp = call_llm(prompt)
        try:
            out = extract_json(resp)
            for url, obj in out.items():
                mapping[url] = obj.get("classes", ["None"]) or ["None"]
        except Exception as exc:
            log.warning("⚠️ Failed batch classification: %s", exc)

    # heuristics: mark scrolling
    for e in batch:
        if any(bad in e.url for bad in scrolling_list):
            mapping.setdefault(e.url, [])
            if "Scrolling" not in mapping[e.url]:
                mapping[e.url].append("Scrolling")

    return mapping


def _train_local_classifier(
    titles: List[str], labels: List[List[str]], model_path: str
) -> Tuple[Pipeline, MultiLabelBinarizer]:
    mlb = MultiLabelBinarizer()
    y = mlb.fit_transform(labels)

    clf = Pipeline(
        [
            ("tfidf", TfidfVectorizer(max_features=20_000, ngram_range=(1, 2))),
            ("clf", OneVsRestClassifier(LogisticRegression(max_iter=1000))),
        ]
    )
    clf.fit(titles, y)
    joblib.dump((clf, mlb), model_path)
    log.info("✅ Classifier trained and saved to %s", model_path)
    return clf, mlb


def _load_local_classifier(model_path: str) -> Tuple[Optional[Pipeline], Optional[MultiLabelBinarizer]]:
    if os.path.exists(model_path):
        log.info("📂 Loading classifier from %s", model_path)
        return joblib.load(model_path)
    return None, None