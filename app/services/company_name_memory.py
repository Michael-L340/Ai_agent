from __future__ import annotations

import json
import re
import threading
from hashlib import sha1
from pathlib import Path
from typing import Any

from app.core.config import ROOT_DIR


DEFAULT_FEEDBACK_FILE = ROOT_DIR / "data" / "company_name_feedback.json"


class CompanyNameFeedbackStore:
    """
    记录人类对“这是不是公司名”的纠错记忆。

    设计目标：
    - 负反馈：某个字段/短语不是公司名
    - 正反馈：某个噪音标题应该归一到哪个公司名
    - 供 EntityVerifier 在下一次解析时优先参考
    """

    def __init__(self, path: Path | None = None):
        self.path = Path(path or DEFAULT_FEEDBACK_FILE)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ensure_file()

    def record_not_company(self, phrase: str, *, note: str = "") -> dict[str, Any]:
        phrase = self._clean_phrase(phrase)
        if not phrase:
            return {}
        now = self._now_iso()
        entry = {
            "kind": "not_company",
            "phrase": phrase,
            "normalized_source": self._normalize_key(phrase),
            "match_mode": self._default_match_mode(phrase),
            "note": note.strip(),
            "created_at": now,
            "updated_at": now,
        }
        return self._upsert("blocked_phrases", entry)

    def record_company_alias(
        self,
        source_text: str,
        normalized_name: str,
        *,
        official_domain: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        source_text = self._clean_phrase(source_text)
        normalized_name = self._clean_phrase(normalized_name)
        if not source_text or not normalized_name:
            return {}
        now = self._now_iso()
        entry = {
            "kind": "company_alias",
            "source_text": source_text,
            "normalized_source": self._normalize_key(source_text),
            "normalized_name": normalized_name,
            "official_domain": self._clean_phrase(official_domain).lower().strip(),
            "match_mode": self._default_match_mode(source_text, prefer_substring=True),
            "note": note.strip(),
            "created_at": now,
            "updated_at": now,
        }
        return self._upsert("company_aliases", entry)

    def analyze(self, *, raw_title: str, snippet: str = "", url: str = "") -> dict[str, Any]:
        payload = self.snapshot()
        raw_title = self._clean_phrase(raw_title)
        snippet = self._clean_phrase(snippet)
        url = self._clean_phrase(url)

        normalized_texts = [
            self._normalize_key(raw_title),
            self._normalize_key(snippet),
            self._normalize_key(url),
        ]

        blocked_matches = self._match_entries(payload.get("blocked_phrases", []), normalized_texts)
        alias_matches = self._match_entries(payload.get("company_aliases", []), normalized_texts)

        cleaned_title = self._strip_entries(raw_title, blocked_matches)
        cleaned_snippet = self._strip_entries(snippet, blocked_matches)
        alias_match = alias_matches[0] if alias_matches else None

        return {
            "fingerprint": self.fingerprint(payload),
            "raw_title": raw_title,
            "snippet": snippet,
            "url": url,
            "cleaned_title": cleaned_title,
            "cleaned_snippet": cleaned_snippet,
            "blocked_phrases": blocked_matches,
            "alias_match": alias_match,
            "alias_matches": alias_matches,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._load()

    def fingerprint(self, payload: dict[str, Any] | None = None) -> str:
        payload = payload or self.snapshot()
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return sha1(canonical.encode("utf-8")).hexdigest()

    def _ensure_file(self) -> None:
        with self._lock:
            if self.path.exists():
                return
            self._save(
                {
                    "version": 1,
                    "updated_at": self._now_iso(),
                    "blocked_phrases": [],
                    "company_aliases": [],
                }
            )

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            self._ensure_file()
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            payload = {
                "version": 1,
                "updated_at": self._now_iso(),
                "blocked_phrases": [],
                "company_aliases": [],
            }
        payload.setdefault("version", 1)
        payload.setdefault("updated_at", self._now_iso())
        payload.setdefault("blocked_phrases", [])
        payload.setdefault("company_aliases", [])
        return payload

    def _save(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["updated_at"] = self._now_iso()
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        tmp_path.replace(self.path)

    def _upsert(self, section: str, entry: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            items = list(payload.get(section, []))
            normalized_source = str(entry.get("normalized_source") or "").strip()

            for index, existing in enumerate(items):
                if str(existing.get("normalized_source") or "").strip() != normalized_source:
                    continue

                merged = dict(existing)
                for key, value in entry.items():
                    if value not in ("", None):
                        merged[key] = value
                merged["updated_at"] = self._now_iso()
                items[index] = merged
                payload[section] = items
                self._save(payload)
                return merged

            items.append(entry)
            payload[section] = items
            self._save(payload)
            return entry

    def _match_entries(self, entries: list[dict[str, Any]], normalized_texts: list[str]) -> list[dict[str, Any]]:
        matched: list[dict[str, Any]] = []
        for entry in entries:
            source_key = str(entry.get("normalized_source") or "").strip()
            if not source_key:
                continue
            mode = str(entry.get("match_mode") or "substring").strip().lower()
            for normalized_text in normalized_texts:
                if not normalized_text:
                    continue
                if mode == "exact":
                    if normalized_text == source_key:
                        matched.append(entry)
                        break
                else:
                    if source_key in normalized_text:
                        matched.append(entry)
                        break

        matched.sort(key=lambda item: len(str(item.get("normalized_source") or "")), reverse=True)
        return matched

    def _strip_entries(self, text: str, entries: list[dict[str, Any]]) -> str:
        cleaned = str(text or "")
        for entry in entries:
            phrase = str(entry.get("phrase") or entry.get("source_text") or "").strip()
            if not phrase:
                continue
            cleaned = re.sub(re.escape(phrase), " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_：:，,。！？?!")
        return cleaned

    def _clean_phrase(self, text: str) -> str:
        cleaned = str(text or "").strip()
        cleaned = re.sub(r"^[\s\"'“”`]+", "", cleaned)
        cleaned = re.sub(r"[\s\"'“”`]+$", "", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    def _normalize_key(self, text: str) -> str:
        lowered = str(text or "").casefold()
        lowered = re.sub(r"[^\w\u4e00-\u9fff]+", " ", lowered)
        lowered = re.sub(r"\s+", " ", lowered).strip()
        return lowered

    def _default_match_mode(self, phrase: str, *, prefer_substring: bool = False) -> str:
        normalized = self._normalize_key(phrase)
        token_count = len([token for token in normalized.split(" ") if token])
        if prefer_substring:
            return "substring"
        if token_count <= 1 or len(normalized) <= 3:
            return "exact"
        return "substring"

    @staticmethod
    def _now_iso() -> str:
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()
