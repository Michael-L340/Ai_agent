from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha1
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from app.models.dd_report import DDOverall, DDProfile, DDReport
from app.models.planner_compaction_run import PlannerCompactionRun
from app.models.planner_feedback_event import PlannerFeedbackEvent
from app.models.scoring_result import ScoringResult
from app.models.scoring_policy import ScoringPolicy


ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_SQLITE_PATH = ROOT_DIR / "data" / "agent_local.db"
MOJIBAKE_MARKERS = ("Ã", "Â", "æ", "ç", "å", "ä", "é", "è", "ê", "ï", "ð", "�")


class DatabaseConfigError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def _sqlite_param(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _text_quality_score(text: str) -> int:
    cjk_count = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    mojibake_count = sum(text.count(marker) for marker in MOJIBAKE_MARKERS)
    control_count = sum(1 for ch in text if ord(ch) < 32 and ch not in "\r\n\t")
    return (cjk_count * 4) - (mojibake_count * 3) - (control_count * 2)


def repair_mojibake_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = text.lstrip("\ufeff")
    if not any(marker in text for marker in MOJIBAKE_MARKERS) and not any(ord(ch) < 32 and ch not in "\r\n\t" for ch in text):
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except Exception:
        return text
    return repaired if _text_quality_score(repaired) > _text_quality_score(text) else text


def normalize_text_content(value: Any) -> str:
    text = repair_mojibake_text(value)
    if not text:
        return ""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def normalize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): normalize_json_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [normalize_json_payload(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return normalize_text_content(value)
    return value


class _SQLiteCursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def __enter__(self) -> "_SQLiteCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._cursor.close()

    def execute(self, sql: str, params: Any = ()):
        sql = sql.replace("%s", "?")
        if isinstance(params, dict):
            adapted_params = {k: _sqlite_param(v) for k, v in params.items()}
        elif isinstance(params, (list, tuple)):
            adapted_params = [_sqlite_param(v) for v in params]
        elif params is None:
            adapted_params = ()
        else:
            adapted_params = _sqlite_param(params)
        return self._cursor.execute(sql, adapted_params)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int:
        return int(self._cursor.lastrowid or 0)


class _SQLiteConnection:
    def __init__(self, path: Path):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")

    def cursor(self) -> _SQLiteCursor:
        return _SQLiteCursor(self._conn.cursor())

    def executescript(self, sql: str) -> None:
        self._conn.executescript(sql)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


class Database:
    CONTENT_TITLE_MARKERS = (
        "what is",
        "how to",
        "why ",
        "new frontier",
        "startups to know",
        "market report",
        "funding data",
        "listicle",
        "explainer",
        "analysis",
        "research",
        "guide",
        "tutorial",
        "podcast",
        "webinar",
        "article",
        "blog",
        "github",
        "open source",
        "playbook for",
        "actually adopting",
        "manage genai risks",
        "get contacts",
        "funded by",
        "buys",
        "acquires",
        "acquisition",
        "deal push",
        "startups in",
        "cybersecurity startups",
        "agentic security startups",
        "cool ",
    )
    SINGLE_TOKEN_BLOCKLIST = {
        "actually",
        "article",
        "blog",
        "closing",
        "in",
        "firewall",
        "genai",
        "of",
        "and",
        "light",
        "mcp",
        "or",
        "the",
        "on",
        "at",
        "by",
        "a",
        "an",
        "guide",
        "hot",
        "most",
        "non",
        "new",
        "podcast",
        "report",
        "research",
        "runtime",
        "top",
        "competitors",
        "team",
        "investors",
        "tutorial",
        "webinar",
        "what",
        "why",
        "how",
        "get",
        "manage",
        "contact",
        "contacts",
        "analysis",
        "explainer",
    }
    GENERIC_PUBLISHER_DOMAINS = (
        "a16z.news",
        "businesswire.com",
        "bloomberg.com",
        "cnbc.com",
        "forbes.com",
        "globenewswire.com",
        "merriam-webster.com",
        "medium.com",
        "news.ycombinator.com",
        "prnewswire.com",
        "reuters.com",
        "siliconangle.com",
        "substack.com",
        "techcrunch.com",
        "venturebeat.com",
        "wired.com",
        "wikipedia.org",
        "youtube.com",
        "reddit.com",
        "x.com",
        "twitter.com",
        "github.com",
    )

    def __init__(self, database_url: str, *, mvp_mode: bool = False):
        self.database_url = database_url.strip()
        self.backend = "postgres"
        self.sqlite_path = LOCAL_SQLITE_PATH
        self.mvp_mode = bool(mvp_mode)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.database_url:
            self.backend = "sqlite"
            print(f"[db] DATABASE_URL missing, using local SQLite fallback at {self.sqlite_path}")
            return

        try:
            with psycopg.connect(self.database_url, connect_timeout=5):
                pass
        except Exception as exc:
            self.backend = "sqlite"
            print(
                f"[db] PostgreSQL unavailable ({exc}); "
                f"using local SQLite fallback at {self.sqlite_path}"
            )

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        if self.backend == "sqlite":
            conn = _SQLiteConnection(self.sqlite_path)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            return

        conn = psycopg.connect(self.database_url)
        conn.row_factory = dict_row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _schema_sql(self) -> str:
        if self.backend == "sqlite":
            return """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                company_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'new',
                stage TEXT NOT NULL DEFAULT 'sourcing',
                description TEXT NOT NULL DEFAULT '',
                thesis_tags TEXT NOT NULL DEFAULT '',
                sources TEXT NOT NULL DEFAULT '',
                raw_title TEXT NOT NULL DEFAULT '',
                candidate_name TEXT NOT NULL DEFAULT '',
                normalized_name TEXT NOT NULL DEFAULT '',
                entity_type TEXT NOT NULL DEFAULT 'unknown',
                official_domain TEXT NOT NULL DEFAULT '',
                verification_status TEXT NOT NULL DEFAULT 'pending_review',
                verification_score REAL NOT NULL DEFAULT 0,
                reject_reason TEXT NOT NULL DEFAULT '',
                resolution_json TEXT NOT NULL DEFAULT '{}',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                source TEXT NOT NULL,
                query TEXT NOT NULL,
                title TEXT NOT NULL,
                snippet TEXT NOT NULL,
                url TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE (source, url),
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS dd_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL UNIQUE,
                business_summary TEXT NOT NULL,
                team_summary TEXT NOT NULL,
                funding_summary TEXT NOT NULL,
                traction_summary TEXT NOT NULL,
                industry_position TEXT NOT NULL,
                business_profile_json TEXT NOT NULL DEFAULT '{}',
                team_profile_json TEXT NOT NULL DEFAULT '{}',
                funding_profile_json TEXT NOT NULL DEFAULT '{}',
                traction_profile_json TEXT NOT NULL DEFAULT '{}',
                market_position_json TEXT NOT NULL DEFAULT '{}',
                dd_overall_json TEXT NOT NULL DEFAULT '{}',
                questions_json TEXT NOT NULL DEFAULT '[]',
                source_hits INTEGER NOT NULL DEFAULT 0,
                completeness_score REAL NOT NULL DEFAULT 0,
                dd_status TEXT NOT NULL DEFAULT 'dd_pending_review',
                evidence_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS dd_feedback_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                lead_id INTEGER,
                company_key TEXT NOT NULL DEFAULT '',
                company_name TEXT NOT NULL DEFAULT '',
                normalized_name TEXT NOT NULL DEFAULT '',
                official_domain TEXT NOT NULL DEFAULT '',
                dimension TEXT NOT NULL DEFAULT 'entity',
                feedback_kind TEXT NOT NULL DEFAULT 'note',
                content TEXT NOT NULL,
                parsed_json TEXT NOT NULL DEFAULT '{}',
                source_question_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS dd_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                lead_id INTEGER NOT NULL,
                company_key TEXT NOT NULL DEFAULT '',
                company_name TEXT NOT NULL DEFAULT '',
                normalized_name TEXT NOT NULL DEFAULT '',
                official_domain TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT 'lead',
                scope_key TEXT NOT NULL DEFAULT '',
                dimension TEXT NOT NULL DEFAULT 'entity',
                question_type TEXT NOT NULL DEFAULT 'missing_fields',
                prompt TEXT NOT NULL,
                missing_fields TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'open',
                answer_text TEXT NOT NULL DEFAULT '',
                answer_feedback_id INTEGER,
                published_at TEXT,
                resolved_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL UNIQUE,
                base_score REAL NOT NULL,
                thesis_fit REAL NOT NULL,
                evidence_strength REAL NOT NULL,
                business_score REAL NOT NULL DEFAULT 0,
                team_score REAL NOT NULL DEFAULT 0,
                funding_score REAL NOT NULL DEFAULT 0,
                traction_score REAL NOT NULL DEFAULT 0,
                market_score REAL NOT NULL DEFAULT 0,
                thesis_fit_score REAL NOT NULL DEFAULT 0,
                evidence_score REAL NOT NULL DEFAULT 0,
                raw_score REAL NOT NULL DEFAULT 0,
                confidence_multiplier REAL NOT NULL DEFAULT 1,
                boost_score REAL NOT NULL DEFAULT 0,
                penalty_score REAL NOT NULL DEFAULT 0,
                final_score REAL NOT NULL,
                score_reason TEXT NOT NULL,
                recommendation_band TEXT NOT NULL DEFAULT 'Reject',
                recommendation_reason TEXT NOT NULL DEFAULT '',
                thesis_fit_breakdown_json TEXT NOT NULL DEFAULT '{}',
                matched_policy_rules_json TEXT NOT NULL DEFAULT '[]',
                policy_version INTEGER NOT NULL DEFAULT 1,
                score_breakdown_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                verdict TEXT NOT NULL,
                feedback_type TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS scoring_policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_key TEXT NOT NULL UNIQUE,
                version INTEGER NOT NULL,
                weights_json TEXT NOT NULL,
                boost_rules_json TEXT NOT NULL,
                penalty_rules_json TEXT NOT NULL,
                source_feedback_id INTEGER,
                source_feedback_type TEXT NOT NULL DEFAULT '',
                source_verdict TEXT NOT NULL DEFAULT '',
                source_scope TEXT NOT NULL DEFAULT '',
                source_scope_key TEXT NOT NULL DEFAULT '',
                source_lead_id INTEGER,
                source_company_key TEXT NOT NULL DEFAULT '',
                source_content TEXT NOT NULL DEFAULT '',
                change_summary TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scoring_policy_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_key TEXT NOT NULL,
                version INTEGER NOT NULL,
                feedback_id INTEGER,
                lead_id INTEGER,
                company_key TEXT NOT NULL DEFAULT '',
                company_name TEXT NOT NULL DEFAULT '',
                normalized_name TEXT NOT NULL DEFAULT '',
                official_domain TEXT NOT NULL DEFAULT '',
                feedback_type TEXT NOT NULL DEFAULT '',
                verdict TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT '',
                scope_key TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                change_json TEXT NOT NULL DEFAULT '{}',
                change_summary TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_long (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_key TEXT NOT NULL UNIQUE,
                memory_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_short (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                memory_date TEXT NOT NULL UNIQUE,
                strategy_value TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS planner_feedback_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_feedback_id INTEGER,
                feedback_type TEXT NOT NULL,
                target TEXT NOT NULL,
                value TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS planner_feedback_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_feedback_id INTEGER,
                feedback_type TEXT NOT NULL,
                target TEXT NOT NULL,
                value TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                merged INTEGER NOT NULL DEFAULT 0,
                merge_summary TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS planner_compactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                compaction_value TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS planner_compaction_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_value TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                sent_at TEXT
            );

            CREATE TABLE IF NOT EXISTS conversation_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                source TEXT NOT NULL,
                session_key TEXT NOT NULL DEFAULT '',
                channel_id TEXT NOT NULL DEFAULT '',
                sender TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL,
                action TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            """

        return """
        CREATE TABLE IF NOT EXISTS leads (
            id BIGSERIAL PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'new',
            stage TEXT NOT NULL DEFAULT 'sourcing',
            description TEXT NOT NULL DEFAULT '',
            thesis_tags TEXT NOT NULL DEFAULT '',
            sources TEXT NOT NULL DEFAULT '',
            raw_title TEXT NOT NULL DEFAULT '',
            candidate_name TEXT NOT NULL DEFAULT '',
            normalized_name TEXT NOT NULL DEFAULT '',
            entity_type TEXT NOT NULL DEFAULT 'unknown',
            official_domain TEXT NOT NULL DEFAULT '',
            verification_status TEXT NOT NULL DEFAULT 'pending_review',
            verification_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            reject_reason TEXT NOT NULL DEFAULT '',
            resolution_json TEXT NOT NULL DEFAULT '{}',
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS signals (
            id BIGSERIAL PRIMARY KEY,
            lead_id BIGINT,
            source TEXT NOT NULL,
            query TEXT NOT NULL,
            title TEXT NOT NULL,
            snippet TEXT NOT NULL,
            url TEXT NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE (source, url),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS dd_reports (
            id BIGSERIAL PRIMARY KEY,
            lead_id BIGINT NOT NULL UNIQUE,
            business_summary TEXT NOT NULL,
            team_summary TEXT NOT NULL,
            funding_summary TEXT NOT NULL,
            traction_summary TEXT NOT NULL,
            industry_position TEXT NOT NULL,
            business_profile_json TEXT NOT NULL DEFAULT '{}',
            team_profile_json TEXT NOT NULL DEFAULT '{}',
            funding_profile_json TEXT NOT NULL DEFAULT '{}',
            traction_profile_json TEXT NOT NULL DEFAULT '{}',
            market_position_json TEXT NOT NULL DEFAULT '{}',
            dd_overall_json TEXT NOT NULL DEFAULT '{}',
            questions_json TEXT NOT NULL DEFAULT '[]',
            source_hits INTEGER NOT NULL DEFAULT 0,
            completeness_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            dd_status TEXT NOT NULL DEFAULT 'dd_pending_review',
            evidence_json TEXT NOT NULL DEFAULT '{}',
            updated_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS dd_feedback_memory (
            id BIGSERIAL PRIMARY KEY,
            scope TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            lead_id BIGINT,
            company_key TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            normalized_name TEXT NOT NULL DEFAULT '',
            official_domain TEXT NOT NULL DEFAULT '',
            dimension TEXT NOT NULL DEFAULT 'entity',
            feedback_kind TEXT NOT NULL DEFAULT 'note',
            content TEXT NOT NULL,
            parsed_json TEXT NOT NULL DEFAULT '{}',
            source_question_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS dd_questions (
            id BIGSERIAL PRIMARY KEY,
            dedupe_key TEXT NOT NULL UNIQUE,
            lead_id BIGINT NOT NULL,
            company_key TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            normalized_name TEXT NOT NULL DEFAULT '',
            official_domain TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT 'lead',
            scope_key TEXT NOT NULL DEFAULT '',
            dimension TEXT NOT NULL DEFAULT 'entity',
            question_type TEXT NOT NULL DEFAULT 'missing_fields',
            prompt TEXT NOT NULL,
            missing_fields TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'open',
            answer_text TEXT NOT NULL DEFAULT '',
            answer_feedback_id BIGINT,
            published_at TIMESTAMPTZ,
            resolved_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS scores (
            id BIGSERIAL PRIMARY KEY,
            lead_id BIGINT NOT NULL UNIQUE,
            base_score DOUBLE PRECISION NOT NULL,
            thesis_fit DOUBLE PRECISION NOT NULL,
            evidence_strength DOUBLE PRECISION NOT NULL,
            business_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            team_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            funding_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            traction_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            market_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            thesis_fit_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            evidence_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            raw_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            confidence_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1,
            boost_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            penalty_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            final_score DOUBLE PRECISION NOT NULL,
            score_reason TEXT NOT NULL,
            recommendation_band TEXT NOT NULL DEFAULT 'Reject',
            recommendation_reason TEXT NOT NULL DEFAULT '',
            thesis_fit_breakdown_json TEXT NOT NULL DEFAULT '{}',
            matched_policy_rules_json TEXT NOT NULL DEFAULT '[]',
            policy_version INTEGER NOT NULL DEFAULT 1,
            score_breakdown_json TEXT NOT NULL DEFAULT '{}',
            updated_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS feedback (
            id BIGSERIAL PRIMARY KEY,
            lead_id BIGINT,
            verdict TEXT NOT NULL,
            feedback_type TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE TABLE IF NOT EXISTS scoring_policies (
            id BIGSERIAL PRIMARY KEY,
            policy_key TEXT NOT NULL UNIQUE,
            version INTEGER NOT NULL,
            weights_json TEXT NOT NULL,
            boost_rules_json TEXT NOT NULL,
            penalty_rules_json TEXT NOT NULL,
            source_feedback_id BIGINT,
            source_feedback_type TEXT NOT NULL DEFAULT '',
            source_verdict TEXT NOT NULL DEFAULT '',
            source_scope TEXT NOT NULL DEFAULT '',
            source_scope_key TEXT NOT NULL DEFAULT '',
            source_lead_id BIGINT,
            source_company_key TEXT NOT NULL DEFAULT '',
            source_content TEXT NOT NULL DEFAULT '',
            change_summary TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scoring_policy_events (
            id BIGSERIAL PRIMARY KEY,
            policy_key TEXT NOT NULL,
            version INTEGER NOT NULL,
            feedback_id BIGINT,
            lead_id BIGINT,
            company_key TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            normalized_name TEXT NOT NULL DEFAULT '',
            official_domain TEXT NOT NULL DEFAULT '',
            feedback_type TEXT NOT NULL DEFAULT '',
            verdict TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            scope_key TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            change_json TEXT NOT NULL DEFAULT '{}',
            change_summary TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_long (
            id BIGSERIAL PRIMARY KEY,
            memory_key TEXT NOT NULL UNIQUE,
            memory_value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_short (
            id BIGSERIAL PRIMARY KEY,
            memory_date TEXT NOT NULL UNIQUE,
            strategy_value TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS planner_feedback_memory (
            id BIGSERIAL PRIMARY KEY,
            source_feedback_id BIGINT,
            feedback_type TEXT NOT NULL,
            target TEXT NOT NULL,
            value TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS planner_feedback_events (
            id BIGSERIAL PRIMARY KEY,
            source_feedback_id BIGINT,
            feedback_type TEXT NOT NULL,
            target TEXT NOT NULL,
            value TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            merged INTEGER NOT NULL DEFAULT 0,
            merge_summary TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS planner_compactions (
            id BIGSERIAL PRIMARY KEY,
            compaction_value TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS planner_compaction_runs (
            id BIGSERIAL PRIMARY KEY,
            run_value TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL,
            sent_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS conversation_messages (
            id BIGSERIAL PRIMARY KEY,
            direction TEXT NOT NULL,
            source TEXT NOT NULL,
            session_key TEXT NOT NULL DEFAULT '',
            channel_id TEXT NOT NULL DEFAULT '',
            sender TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL,
            action TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL
        );
        """

    def init_schema(self) -> None:
        schema_sql = self._schema_sql()

        with self._connect() as conn:
            if self.backend == "sqlite":
                conn.executescript(schema_sql)
            else:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)

        self._ensure_lead_resolution_columns()
        self._ensure_dd_report_columns()
        self._ensure_dd_question_columns()
        self._ensure_score_columns()

    def _existing_columns(self, table_name: str) -> set[str]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if self.backend == "sqlite":
                    cur.execute(f"PRAGMA table_info({table_name})")
                    rows = cur.fetchall()
                    return {str(row["name"]) for row in rows}
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    """,
                    (table_name,),
                )
                return {str(row["column_name"]) for row in cur.fetchall()}

    def _ensure_lead_resolution_columns(self) -> None:
        existing = self._existing_columns("leads")
        columns = [
            ("raw_title", "TEXT NOT NULL DEFAULT ''"),
            ("candidate_name", "TEXT NOT NULL DEFAULT ''"),
            ("normalized_name", "TEXT NOT NULL DEFAULT ''"),
            ("entity_type", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("official_domain", "TEXT NOT NULL DEFAULT ''"),
            ("verification_status", "TEXT NOT NULL DEFAULT 'pending_review'"),
            ("verification_score", "REAL NOT NULL DEFAULT 0"),
            ("reject_reason", "TEXT NOT NULL DEFAULT ''"),
            ("resolution_json", "TEXT NOT NULL DEFAULT '{}'"),
        ]

        with self._connect() as conn:
            with conn.cursor() as cur:
                for column_name, ddl in columns:
                    if column_name in existing:
                        continue
                    if self.backend == "sqlite":
                        cur.execute(f"ALTER TABLE leads ADD COLUMN {column_name} {ddl}")
                    else:
                        cur.execute(f"ALTER TABLE leads ADD COLUMN IF NOT EXISTS {column_name} {ddl}")

                cur.execute(
                    """
                    UPDATE leads
                    SET normalized_name = CASE
                        WHEN normalized_name IS NULL OR normalized_name = '' THEN company_name
                        ELSE normalized_name
                    END,
                    candidate_name = CASE
                        WHEN candidate_name IS NULL OR candidate_name = '' THEN company_name
                        ELSE candidate_name
                    END,
                    raw_title = CASE
                        WHEN raw_title IS NULL THEN company_name
                        ELSE raw_title
                    END
                    """
                )

    def _ensure_dd_report_columns(self) -> None:
        existing = self._existing_columns("dd_reports")
        columns = [
            ("business_profile_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("team_profile_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("funding_profile_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("traction_profile_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("market_position_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("dd_overall_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("questions_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("completeness_score", "REAL NOT NULL DEFAULT 0"),
            ("dd_status", "TEXT NOT NULL DEFAULT 'dd_pending_review'"),
        ]

        with self._connect() as conn:
            with conn.cursor() as cur:
                for column_name, ddl in columns:
                    if column_name in existing:
                        continue
                    if self.backend == "sqlite":
                        cur.execute(f"ALTER TABLE dd_reports ADD COLUMN {column_name} {ddl}")
                    else:
                        cur.execute(f"ALTER TABLE dd_reports ADD COLUMN IF NOT EXISTS {column_name} {ddl}")

                cur.execute(
                    """
                    UPDATE dd_reports
                    SET completeness_score = CASE
                        WHEN COALESCE(completeness_score, 0) > 0 THEN completeness_score
                        ELSE
                            (CASE WHEN COALESCE(business_summary, '') <> '' THEN 20 ELSE 0 END) +
                            (CASE WHEN COALESCE(team_summary, '') <> '' THEN 20 ELSE 0 END) +
                            (CASE WHEN COALESCE(funding_summary, '') <> '' THEN 20 ELSE 0 END) +
                            (CASE WHEN COALESCE(traction_summary, '') <> '' THEN 20 ELSE 0 END) +
                            (CASE WHEN COALESCE(industry_position, '') <> '' THEN 20 ELSE 0 END)
                    END,
                    dd_status = CASE
                        WHEN COALESCE(dd_status, '') IN ('dd_done', 'dd_partial', 'dd_pending_review', 'dd_waiting_human') THEN dd_status
                        WHEN
                            (
                                (CASE WHEN COALESCE(business_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(team_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(funding_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(traction_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(industry_position, '') <> '' THEN 20 ELSE 0 END)
                            ) >= 80
                        THEN 'dd_done'
                        WHEN
                            (
                                (CASE WHEN COALESCE(business_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(team_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(funding_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(traction_summary, '') <> '' THEN 20 ELSE 0 END) +
                                (CASE WHEN COALESCE(industry_position, '') <> '' THEN 20 ELSE 0 END)
                            ) > 0
                        THEN 'dd_partial'
                        ELSE 'dd_pending_review'
                    END
                    """
                )

    def _ensure_dd_question_columns(self) -> None:
        existing = self._existing_columns("dd_questions")
        columns = [
            ("scope", "TEXT NOT NULL DEFAULT 'lead'"),
            ("scope_key", "TEXT NOT NULL DEFAULT ''"),
        ]

        with self._connect() as conn:
            with conn.cursor() as cur:
                for column_name, ddl in columns:
                    if column_name in existing:
                        continue
                    if self.backend == "sqlite":
                        cur.execute(f"ALTER TABLE dd_questions ADD COLUMN {column_name} {ddl}")
                    else:
                        cur.execute(f"ALTER TABLE dd_questions ADD COLUMN IF NOT EXISTS {column_name} {ddl}")

    def _ensure_score_columns(self) -> None:
        existing = self._existing_columns("scores")
        columns = [
            ("business_score", "REAL NOT NULL DEFAULT 0"),
            ("team_score", "REAL NOT NULL DEFAULT 0"),
            ("funding_score", "REAL NOT NULL DEFAULT 0"),
            ("traction_score", "REAL NOT NULL DEFAULT 0"),
            ("market_score", "REAL NOT NULL DEFAULT 0"),
            ("thesis_fit_score", "REAL NOT NULL DEFAULT 0"),
            ("evidence_score", "REAL NOT NULL DEFAULT 0"),
            ("raw_score", "REAL NOT NULL DEFAULT 0"),
            ("confidence_multiplier", "REAL NOT NULL DEFAULT 1"),
            ("boost_score", "REAL NOT NULL DEFAULT 0"),
            ("penalty_score", "REAL NOT NULL DEFAULT 0"),
            ("recommendation_band", "TEXT NOT NULL DEFAULT 'Reject'"),
            ("recommendation_reason", "TEXT NOT NULL DEFAULT ''"),
            ("thesis_fit_breakdown_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("matched_policy_rules_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("policy_version", "INTEGER NOT NULL DEFAULT 1"),
            ("score_breakdown_json", "TEXT NOT NULL DEFAULT '{}'"),
        ]

        with self._connect() as conn:
            with conn.cursor() as cur:
                for column_name, ddl in columns:
                    if column_name in existing:
                        continue
                    if self.backend == "sqlite":
                        cur.execute(f"ALTER TABLE scores ADD COLUMN {column_name} {ddl}")
                    else:
                        cur.execute(f"ALTER TABLE scores ADD COLUMN IF NOT EXISTS {column_name} {ddl}")

    @staticmethod
    def _json_loads(value: Any, default: Any | None = None) -> Any:
        if value is None:
            return {} if default is None else default
        if isinstance(value, (dict, list)):
            return normalize_json_payload(value)
        text = str(value).strip()
        if not text:
            return {} if default is None else default
        try:
            return normalize_json_payload(json.loads(text))
        except Exception:
            return {} if default is None else default

    def _legacy_dd_completeness(self, row: dict[str, Any]) -> float:
        summary_fields = [
            "business_summary",
            "team_summary",
            "funding_summary",
            "traction_summary",
            "industry_position",
        ]
        filled = sum(1 for field in summary_fields if str(row.get(field) or "").strip())
        return round((filled / len(summary_fields)) * 100.0, 2)

    def _profile_from_json_or_summary(
        self,
        *,
        json_value: Any,
        summary_text: str,
        field_template: dict[str, Any],
        summary_key: str,
    ) -> dict[str, Any]:
        parsed = self._json_loads(json_value, default={})
        if isinstance(parsed, dict) and parsed.get("fields"):
            return parsed

        fields = dict(field_template)
        if summary_text:
            if summary_key in fields and isinstance(fields[summary_key], list):
                fields[summary_key] = [summary_text]
            else:
                fields[summary_key] = summary_text
        return {
            "fields": fields,
            "evidence": [],
            "missing_fields": [key for key, value in fields.items() if key != summary_key and self._is_empty_profile_value(value)],
            "confidence": 20.0 if summary_text else 0.0,
        }

    @staticmethod
    def _is_empty_profile_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) == 0
        return False

    @staticmethod
    def _row_value(row: Any, key: str, default: Any = "") -> Any:
        if isinstance(row, dict):
            return row.get(key, default)
        try:
            return row[key]
        except Exception:
            return default

    def _resolved_normalized_name(self, row: Any) -> str:
        normalized_name = str(self._row_value(row, "normalized_name", "") or "").strip()
        candidate_name = str(self._row_value(row, "candidate_name", "") or "").strip()
        company_name = str(self._row_value(row, "company_name", "") or "").strip()
        raw_title = str(self._row_value(row, "raw_title", "") or "").strip()
        verification_status = str(self._row_value(row, "verification_status", "pending_review") or "pending_review").strip().lower()
        entity_type = str(self._row_value(row, "entity_type", "unknown") or "unknown").strip().lower()

        if verification_status in {"verified", "likely_company"} and entity_type == "company":
            return normalized_name or candidate_name or company_name or raw_title
        return normalized_name or candidate_name or company_name or raw_title

    def _display_name_from_row(self, row: Any) -> str:
        normalized_name = self._resolved_normalized_name(row)
        candidate_name = str(self._row_value(row, "candidate_name", "") or "").strip()
        company_name = str(self._row_value(row, "company_name", "") or "").strip()
        raw_title = str(self._row_value(row, "raw_title", "") or "").strip()
        verification_status = str(self._row_value(row, "verification_status", "pending_review") or "pending_review").strip().lower()
        entity_type = str(self._row_value(row, "entity_type", "unknown") or "unknown").strip().lower()

        if verification_status in {"verified", "likely_company"} and entity_type == "company":
            return normalized_name or candidate_name or company_name or raw_title
        return normalized_name or candidate_name or company_name or raw_title

    def _eligible_company_statuses(self) -> tuple[str, ...]:
        return ("verified", "likely_company") if self.mvp_mode else ("verified",)

    def _scoring_ready_condition_sql(self, *, lead_alias: str = "l", dd_alias: str = "d") -> str:
        verification_expr = f"COALESCE({lead_alias}.verification_status, 'pending_review')"
        dd_status_expr = f"COALESCE({dd_alias}.dd_status, 'dd_pending_review')"
        if self.mvp_mode:
            return (
                f"(({verification_expr} = 'verified' AND {dd_status_expr} IN ('dd_partial', 'dd_done')) "
                f"OR ({verification_expr} = 'likely_company' AND {dd_status_expr} = 'dd_partial'))"
            )
        return f"({verification_expr} = 'verified' AND {dd_status_expr} IN ('dd_partial', 'dd_done'))"

    def _lead_state_from_verification(self, verification_status: str) -> tuple[str, str]:
        normalized = str(verification_status or "").strip().lower()
        if normalized in {"verified", "likely_company"}:
            return "new", "sourcing"
        if normalized == "rejected":
            return "rejected", "review"
        return "pending_review", "review"

    def _with_display_name(self, row: Any) -> dict[str, Any]:
        record = dict(row)
        record["normalized_name"] = self._resolved_normalized_name(record)
        record["display_name"] = self._display_name_from_row(record)
        return record

    def _hydrate_dd_record(self, row: dict[str, Any]) -> dict[str, Any]:
        business_profile = self._profile_from_json_or_summary(
            json_value=row.get("business_profile_json"),
            summary_text=str(row.get("business_summary") or ""),
            field_template={
                "one_liner": "",
                "products_services": [],
                "target_customers": [],
                "use_cases": [],
                "official_domain": str(row.get("official_domain") or ""),
            },
            summary_key="one_liner",
        )
        team_profile = self._profile_from_json_or_summary(
            json_value=row.get("team_profile_json"),
            summary_text=str(row.get("team_summary") or ""),
            field_template={
                "founders": [],
                "key_people": [],
                "prior_companies": [],
                "research_background": [],
            },
            summary_key="key_people",
        )
        funding_profile = self._profile_from_json_or_summary(
            json_value=row.get("funding_profile_json"),
            summary_text=str(row.get("funding_summary") or ""),
            field_template={
                "founded_year": "",
                "headquarters": "",
                "funding_rounds": [],
                "total_raised": "",
                "valuation": "",
                "notable_investors": [],
            },
            summary_key="funding_rounds",
        )
        traction_profile = self._profile_from_json_or_summary(
            json_value=row.get("traction_profile_json"),
            summary_text=str(row.get("traction_summary") or ""),
            field_template={
                "customers": [],
                "partners": [],
                "product_launches": [],
                "revenue_signals": [],
                "deployment_signals": [],
            },
            summary_key="product_launches",
        )
        market_position = self._profile_from_json_or_summary(
            json_value=row.get("market_position_json"),
            summary_text=str(row.get("industry_position") or ""),
            field_template={
                "sub_sector": [],
                "is_new_category": None,
                "competitors": [],
                "leader_signals": [],
                "crowdedness": "",
            },
            summary_key="sub_sector",
        )
        dd_overall = self._json_loads(row.get("dd_overall_json"), default={})
        questions = self._json_loads(row.get("questions_json"), default=[])
        thesis_fit_breakdown = self._json_loads(row.get("thesis_fit_breakdown_json"), default={})
        matched_policy_rules = self._json_loads(row.get("matched_policy_rules_json"), default=[])
        score_breakdown = self._json_loads(row.get("score_breakdown_json"), default={})
        if not isinstance(dd_overall, dict) or not dd_overall:
            completeness_score = float(row.get("completeness_score") or self._legacy_dd_completeness(row) or 0.0)
            dd_status = str(row.get("dd_status") or "dd_pending_review")
            dd_overall = {
                "dd_status": dd_status,
                "completeness_score": completeness_score,
                "source_hits": int(row.get("source_hits") or 0),
                "summary": "Legacy DD report; structured fields may be sparse until re-enriched.",
                "missing_dimensions": [],
                "confidence": 0.0,
                "generated_at": str(row.get("dd_updated_at") or ""),
            }

        row = dict(row)
        row["business_profile"] = business_profile
        row["team_profile"] = team_profile
        row["funding_profile"] = funding_profile
        row["traction_profile"] = traction_profile
        row["market_position"] = market_position
        row["dd_overall"] = dd_overall
        row["questions"] = questions if isinstance(questions, list) else []
        row["thesis_fit_breakdown"] = thesis_fit_breakdown if isinstance(thesis_fit_breakdown, dict) else {}
        row["matched_policy_rules"] = matched_policy_rules if isinstance(matched_policy_rules, list) else []
        row["score_breakdown"] = score_breakdown if isinstance(score_breakdown, dict) else {}
        row["dd_status"] = str(row.get("dd_status") or dd_overall.get("dd_status") or "dd_pending_review")
        row["completeness_score"] = float(row.get("completeness_score") or dd_overall.get("completeness_score") or 0.0)
        return self._with_display_name(row)

    def purge_demo_data(self) -> dict[str, int]:
        """
        Remove legacy demo rows that were seeded with example.com URLs.
        Safe to call repeatedly; it only targets known demo artifacts.
        """
        demo_names = (
            "ShieldAgent raises seed round for enterpr",
            "AgentArmor completes angel financing to s",
        )
        counts = {
            "leads": 0,
            "signals": 0,
            "dd_reports": 0,
            "scores": 0,
            "feedback": 0,
        }

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM leads
                    WHERE company_name IN (%s, %s)
                       OR EXISTS (
                           SELECT 1
                           FROM signals s
                           WHERE s.lead_id = leads.id
                             AND s.url LIKE %s
                       )
                    """,
                    (demo_names[0], demo_names[1], "%example.com%"),
                )
                demo_ids = [int(row["id"]) for row in cur.fetchall()]

                if not demo_ids:
                    return counts

                placeholders = ",".join(["%s"] * len(demo_ids))
                params = tuple(demo_ids)

                cur.execute(
                    f"DELETE FROM feedback WHERE lead_id IN ({placeholders})",
                    params,
                )
                counts["feedback"] = int(cur.rowcount or 0)

                cur.execute(
                    f"DELETE FROM scores WHERE lead_id IN ({placeholders})",
                    params,
                )
                counts["scores"] = int(cur.rowcount or 0)

                cur.execute(
                    f"DELETE FROM dd_reports WHERE lead_id IN ({placeholders})",
                    params,
                )
                counts["dd_reports"] = int(cur.rowcount or 0)

                cur.execute(
                    f"DELETE FROM signals WHERE lead_id IN ({placeholders}) OR url LIKE %s",
                    params + ("%example.com%",),
                )
                counts["signals"] = int(cur.rowcount or 0)

                cur.execute(
                    f"DELETE FROM leads WHERE id IN ({placeholders})",
                    params,
                )
                counts["leads"] = int(cur.rowcount or 0)

        return counts

    def upsert_lead(
        self,
        company_name: str,
        source: str,
        description: str = "",
        thesis_tags: str = "",
        *,
        resolution: dict[str, Any] | None = None,
    ) -> tuple[int, bool]:
        resolution = resolution or {}
        normalized_name = str(resolution.get("normalized_name") or company_name or "").strip()
        candidate_name = str(resolution.get("candidate_name") or normalized_name or company_name or "").strip()
        raw_title = str(resolution.get("raw_title") or "").strip()
        entity_type = str(resolution.get("entity_type") or "unknown").strip().lower() or "unknown"
        official_domain = str(resolution.get("official_domain") or "").strip().lower()
        verification_status = str(resolution.get("verification_status") or "pending_review").strip().lower() or "pending_review"
        reject_reason = str(resolution.get("reject_reason") or "").strip()
        verification_score = float(resolution.get("verification_score") or 0.0)
        resolution_json = json.dumps(resolution, ensure_ascii=False)
        key = self._company_key_from_name(normalized_name or company_name)
        content_like = self._resolution_looks_content_like(
            raw_title=raw_title,
            candidate_name=candidate_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            reject_reason=reject_reason,
        )
        now = utc_now()

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, company_name, sources, description, thesis_tags, raw_title,
                        candidate_name, normalized_name, entity_type, official_domain,
                        verification_status, verification_score, reject_reason, resolution_json
                    FROM leads
                    WHERE company_key = %s
                    """,
                    (key,),
                )
                row = cur.fetchone()

                if row:
                    old_sources = {s.strip() for s in str(row["sources"]).split(",") if s.strip()}
                    old_sources.add(source)
                    merged_sources = ",".join(sorted(old_sources))
                    merged_desc = description or str(row["description"])
                    merged_tags = thesis_tags or str(row["thesis_tags"])
                    existing_status = str(row["verification_status"] or "pending_review")
                    existing_rank = self._verification_rank(existing_status)
                    new_rank = self._verification_rank(verification_status)
                    merged_score = max(float(row["verification_score"] or 0.0), verification_score)
                    merged_entity_type = str(row["entity_type"] or "unknown")
                    merged_official_domain = str(row["official_domain"] or "")
                    merged_raw_title = raw_title or str(row["raw_title"] or "")
                    merged_candidate_name = candidate_name or str(row["candidate_name"] or "")
                    merged_normalized_name = normalized_name or str(row["normalized_name"] or row["company_name"] or "")
                    merged_reject_reason = str(row["reject_reason"] or "")
                    merged_resolution_json = str(row["resolution_json"] or "{}")
                    new_domain_key = self._company_key_from_name(official_domain)
                    domain_matches = bool(
                        official_domain
                        and key
                        and new_domain_key
                        and (key in new_domain_key or new_domain_key in key)
                    )
                    merged_status = existing_status

                    if new_rank > existing_rank:
                        merged_status = verification_status
                    elif new_rank < existing_rank:
                        if not domain_matches or content_like or verification_status == "rejected":
                            merged_status = verification_status
                    else:
                        merged_status = verification_status

                    if merged_status == verification_status:
                        if entity_type:
                            merged_entity_type = entity_type
                        if official_domain:
                            merged_official_domain = official_domain
                        if reject_reason:
                            merged_reject_reason = reject_reason
                        merged_resolution_json = resolution_json or merged_resolution_json

                    lead_status, lead_stage = self._lead_state_from_verification(merged_status)

                    cur.execute(
                        """
                        UPDATE leads
                        SET sources = %s,
                            description = %s,
                            thesis_tags = %s,
                            status = %s,
                            stage = %s,
                            company_name = %s,
                            raw_title = %s,
                            candidate_name = %s,
                            normalized_name = %s,
                            entity_type = %s,
                            official_domain = %s,
                            verification_status = %s,
                            verification_score = %s,
                            reject_reason = %s,
                            resolution_json = %s,
                            last_seen_at = %s
                        WHERE id = %s
                        """,
                        (
                            merged_sources,
                            merged_desc,
                            merged_tags,
                            lead_status,
                            lead_stage,
                            merged_normalized_name or merged_candidate_name or str(row["company_name"] or ""),
                            merged_raw_title,
                            merged_candidate_name,
                            merged_normalized_name,
                            merged_entity_type,
                            merged_official_domain,
                            merged_status,
                            merged_score,
                            merged_reject_reason,
                            merged_resolution_json,
                            now,
                            row["id"],
                        ),
                    )
                    return int(row["id"]), False

                cur.execute(
                    """
                    INSERT INTO leads (
                        company_name, company_key, status, stage, description, thesis_tags,
                        sources, raw_title, candidate_name, normalized_name, entity_type,
                        official_domain, verification_status, verification_score, reject_reason,
                        resolution_json, first_seen_at, last_seen_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        normalized_name or candidate_name or company_name.strip(),
                        key,
                        *self._lead_state_from_verification(verification_status),
                        description,
                        thesis_tags,
                        source,
                        raw_title or normalized_name or candidate_name or company_name.strip(),
                        candidate_name or normalized_name or company_name.strip(),
                        normalized_name or candidate_name or company_name.strip(),
                        entity_type,
                        official_domain,
                        verification_status,
                        verification_score,
                        reject_reason,
                        resolution_json,
                        now,
                        now,
                    ),
                )
                inserted = cur.fetchone()
                return int(inserted["id"]), True

    def add_signal(
        self,
        lead_id: int | None,
        source: str,
        query: str,
        title: str,
        snippet: str,
        url: str,
        raw: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO signals (
                        lead_id, source, query, title, snippet, url, fetched_at, raw_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, url) DO NOTHING
                    """,
                    (lead_id, source, query, title, snippet, url, now, json.dumps(raw, ensure_ascii=False)),
                )

    def _verification_rank(self, status: str) -> int:
        normalized = str(status or "").strip().lower()
        return {
            "rejected": 0,
            "pending_review": 1,
            "likely_company": 2,
            "verified": 3,
        }.get(normalized, 1)

    def _company_key_from_name(self, company_name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(company_name or "").strip().lower())

    def _is_generic_domain(self, domain: str) -> bool:
        lowered = str(domain or "").strip().lower()
        return any(
            lowered == item or lowered.endswith(f".{item}") or lowered.endswith(item)
            for item in self.GENERIC_PUBLISHER_DOMAINS
        )

    def _resolution_looks_content_like(
        self,
        *,
        raw_title: str,
        candidate_name: str,
        normalized_name: str,
        official_domain: str,
        reject_reason: str,
    ) -> bool:
        if official_domain and self._is_generic_domain(official_domain):
            return True

        text = f"{raw_title} {candidate_name} {normalized_name} {reject_reason}".lower()
        if any(marker in text for marker in self.CONTENT_TITLE_MARKERS):
            return True

        blocked_names = {
            "microsoft",
            "google",
            "meta",
            "amazon",
            "apple",
            "openai",
            "github",
            "linkedin",
            "crunchbase",
            "pitchbook",
            "the information",
            "theinformation",
            "wikipedia",
            "reddit",
            "youtube",
            "x",
            "twitter",
            "ibm",
            "crowdstrike",
            "sequoia",
            "crn",
            "fortune",
            "cnbc",
            "a16z",
            "yc",
            "y combinator",
            "reuters",
            "bloomberg",
            "forbes",
            "wired",
            "venturebeat",
            "businesswire",
            "prnewswire",
            "globenewswire",
        }
        tokens = [token for token in re.split(r"[\s&/_-]+", (normalized_name or candidate_name or "").lower()) if token]
        if not tokens:
            return True

        if lowered_name := (normalized_name or candidate_name or "").lower().strip():
            if lowered_name in blocked_names:
                return True

        if any(token in blocked_names for token in tokens):
            return True

        if len(tokens) == 1 and tokens[0] in self.SINGLE_TOKEN_BLOCKLIST:
            return True

        if len(tokens) <= 4:
            content_hits = sum(1 for token in tokens if token in self.SINGLE_TOKEN_BLOCKLIST)
            if content_hits >= 2:
                return True

        generic_hits = sum(
            1
            for token in tokens
            if token in {
                "ai",
                "agent",
                "agents",
                "security",
                "platform",
                "solution",
                "software",
                "system",
                "systems",
                "startup",
                "startups",
                "genai",
                "mcp",
                "firewall",
                "light",
                "closing",
            }
        )
        if generic_hits == len(tokens):
            return True

        return False

    def _lead_is_active_company(self, row: Any) -> bool:
        status = str(row["verification_status"] or "pending_review").strip().lower()
        entity_type = str(row["entity_type"] or "unknown").strip().lower()
        if status not in self._eligible_company_statuses() or entity_type != "company":
            return False

        raw_title = str(row["raw_title"] or "")
        candidate_name = str(row["candidate_name"] or "")
        normalized_name = str(row["normalized_name"] or "")
        official_domain = str(row["official_domain"] or "")
        reject_reason = str(row["reject_reason"] or "")
        return not self._resolution_looks_content_like(
            raw_title=raw_title,
            candidate_name=candidate_name,
            normalized_name=normalized_name,
            official_domain=official_domain,
            reject_reason=reject_reason,
        )

    def get_leads_without_dd(self, limit: int = 50) -> list[dict[str, Any]]:
        eligible_statuses = self._eligible_company_statuses()
        status_placeholders = ", ".join(["%s"] * len(eligible_statuses))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.*, COALESCE(d.dd_status, 'dd_pending_review') AS dd_status, d.updated_at AS dd_updated_at
                    FROM leads l
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE (
                        d.id IS NULL
                        OR COALESCE(d.dd_status, 'dd_pending_review') NOT IN ('dd_done', 'dd_waiting_human')
                        OR l.last_seen_at > COALESCE(d.updated_at, l.first_seen_at)
                    )
                      AND COALESCE(l.verification_status, 'pending_review') IN ({status_placeholders})
                      AND COALESCE(l.entity_type, 'unknown') = 'company'
                    ORDER BY l.last_seen_at DESC
                    LIMIT %s
                    """,
                    (*eligible_statuses, limit),
                )
                rows = list(cur.fetchall())
                return [row for row in rows if self._lead_is_active_company(row)]

    def upsert_dd_report(
        self,
        lead_id: int | None = None,
        business_summary: str = "",
        team_summary: str = "",
        funding_summary: str = "",
        traction_summary: str = "",
        industry_position: str = "",
        source_hits: int = 0,
        evidence: dict[str, Any] | None = None,
        *,
        report: DDReport | dict[str, Any] | None = None,
    ) -> None:
        if report is not None:
            if isinstance(report, DDReport):
                payload = report.to_dict()
            elif isinstance(report, dict):
                payload = dict(report)
            else:
                raise TypeError(f"Unsupported report type: {type(report)!r}")

            lead_id = int(payload.get("lead_id") or lead_id or 0)
            if not lead_id:
                raise ValueError("lead_id is required for DD report upsert")

            business_summary = str(payload.get("business_summary") or "")
            team_summary = str(payload.get("team_summary") or "")
            funding_summary = str(payload.get("funding_summary") or "")
            traction_summary = str(payload.get("traction_summary") or "")
            industry_position = str(payload.get("industry_position") or "")
            source_hits = int(payload.get("source_hits") or 0)
            evidence = dict(payload.get("evidence_json") or {})
            business_profile = payload.get("business_profile") or {}
            team_profile = payload.get("team_profile") or {}
            funding_profile = payload.get("funding_profile") or {}
            traction_profile = payload.get("traction_profile") or {}
            market_position = payload.get("market_position") or {}
            dd_overall = payload.get("dd_overall") or {}
            questions = payload.get("questions") or []
            completeness_score = float(payload.get("completeness_score") or 0.0)
            dd_status = str(payload.get("dd_status") or "dd_pending_review")
        else:
            if lead_id is None:
                raise ValueError("lead_id is required for DD report upsert")
            evidence = evidence or {}
            business_profile = {}
            team_profile = {}
            funding_profile = {}
            traction_profile = {}
            market_position = {}
            dd_overall = {}
            questions = []
            completeness_score = 0.0
            dd_status = "dd_done" if source_hits >= 2 else "dd_pending_review"

        business_profile = normalize_json_payload(business_profile)
        team_profile = normalize_json_payload(team_profile)
        funding_profile = normalize_json_payload(funding_profile)
        traction_profile = normalize_json_payload(traction_profile)
        market_position = normalize_json_payload(market_position)
        dd_overall = normalize_json_payload(dd_overall)
        questions = normalize_json_payload(questions)
        evidence = normalize_json_payload(evidence or {})

        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dd_reports (
                        lead_id, business_summary, team_summary, funding_summary,
                        traction_summary, industry_position,
                        business_profile_json, team_profile_json, funding_profile_json,
                        traction_profile_json, market_position_json, dd_overall_json,
                        questions_json, source_hits, completeness_score, dd_status, evidence_json, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lead_id) DO UPDATE SET
                        business_summary = EXCLUDED.business_summary,
                        team_summary = EXCLUDED.team_summary,
                        funding_summary = EXCLUDED.funding_summary,
                        traction_summary = EXCLUDED.traction_summary,
                        industry_position = EXCLUDED.industry_position,
                        business_profile_json = EXCLUDED.business_profile_json,
                        team_profile_json = EXCLUDED.team_profile_json,
                        funding_profile_json = EXCLUDED.funding_profile_json,
                        traction_profile_json = EXCLUDED.traction_profile_json,
                        market_position_json = EXCLUDED.market_position_json,
                        dd_overall_json = EXCLUDED.dd_overall_json,
                        questions_json = EXCLUDED.questions_json,
                        source_hits = EXCLUDED.source_hits,
                        completeness_score = EXCLUDED.completeness_score,
                        dd_status = EXCLUDED.dd_status,
                        evidence_json = EXCLUDED.evidence_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        lead_id,
                        business_summary,
                        team_summary,
                        funding_summary,
                        traction_summary,
                        industry_position,
                        json.dumps(business_profile, ensure_ascii=False),
                        json.dumps(team_profile, ensure_ascii=False),
                        json.dumps(funding_profile, ensure_ascii=False),
                        json.dumps(traction_profile, ensure_ascii=False),
                        json.dumps(market_position, ensure_ascii=False),
                        json.dumps(dd_overall, ensure_ascii=False),
                        json.dumps(questions, ensure_ascii=False),
                        source_hits,
                        completeness_score,
                        dd_status,
                        json.dumps(evidence, ensure_ascii=False),
                        now,
                    ),
                )
                new_status = dd_status
                cur.execute("UPDATE leads SET status = %s, stage = 'dd' WHERE id = %s", (new_status, lead_id))

    def get_lead_by_id(self, lead_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM leads WHERE id = %s", (lead_id,))
                row = cur.fetchone()
                return self._with_display_name(row) if row else None

    def get_scoring_candidates(self, limit: int = 100) -> list[dict[str, Any]]:
        scoring_ready_sql = self._scoring_ready_condition_sql(lead_alias="l", dd_alias="d")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        l.id AS lead_id,
                        l.company_name,
                        l.company_key,
                        l.description,
                        l.thesis_tags,
                        l.sources,
                        l.raw_title,
                        l.candidate_name,
                        l.normalized_name,
                        l.entity_type,
                        l.official_domain,
                        l.verification_status,
                        l.verification_score,
                        l.reject_reason,
                        d.business_summary,
                        d.team_summary,
                        d.funding_summary,
                        d.traction_summary,
                        d.industry_position,
                        d.business_profile_json,
                        d.team_profile_json,
                        d.funding_profile_json,
                        d.traction_profile_json,
                        d.market_position_json,
                        d.dd_overall_json,
                        d.questions_json,
                        d.evidence_json,
                        d.source_hits,
                        d.completeness_score,
                        d.dd_status,
                        d.updated_at AS dd_updated_at
                    FROM leads l
                    INNER JOIN dd_reports d ON d.lead_id = l.id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                    ORDER BY d.updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = list(cur.fetchall())
                return [self._with_display_name(row) for row in rows if self._lead_is_active_company(row)]

    def upsert_score(
        self,
        lead_id: int,
        base_score: float,
        thesis_fit: float,
        evidence_strength: float,
        final_score: float,
        score_reason: str,
        *,
        result: ScoringResult | dict[str, Any] | None = None,
    ) -> None:
        now = utc_now()
        payload: dict[str, Any] = {}
        if result is not None:
            if isinstance(result, ScoringResult):
                payload = result.to_dict()
            elif isinstance(result, dict):
                payload = dict(result)
            else:
                raise TypeError(f"Unsupported score result type: {type(result)!r}")

        business_score = float(payload.get("business_score", 0.0) or 0.0)
        team_score = float(payload.get("team_score", 0.0) or 0.0)
        funding_score = float(payload.get("funding_score", 0.0) or 0.0)
        traction_score = float(payload.get("traction_score", 0.0) or 0.0)
        market_score = float(payload.get("market_score", 0.0) or 0.0)
        thesis_fit_score = float(payload.get("thesis_fit_score", thesis_fit) or 0.0)
        evidence_score = float(payload.get("evidence_score", evidence_strength) or 0.0)
        raw_score = float(payload.get("raw_score", base_score) or 0.0)
        confidence_multiplier = float(payload.get("confidence_multiplier", 1.0) or 1.0)
        boost_score = float(payload.get("boost_score", 0.0) or 0.0)
        penalty_score = float(payload.get("penalty_score", 0.0) or 0.0)
        recommendation_band = str(payload.get("recommendation_band") or "Reject")
        recommendation_reason = str(payload.get("recommendation_reason") or score_reason or "")
        thesis_fit_breakdown = dict(payload.get("thesis_fit_breakdown") or {})
        matched_policy_rules = list(payload.get("matched_policy_rules") or [])
        policy_version = int(payload.get("policy_version") or 1)
        score_breakdown_json = dict(payload.get("component_reasons") or {})
        if payload.get("evidence_snapshot"):
            score_breakdown_json.setdefault("evidence_snapshot", payload.get("evidence_snapshot"))
        if thesis_fit_breakdown:
            score_breakdown_json.setdefault("thesis_fit_breakdown", thesis_fit_breakdown)
        if matched_policy_rules:
            score_breakdown_json.setdefault("matched_policy_rules", matched_policy_rules)
        score_breakdown_json.setdefault("policy_version", policy_version)
        score_breakdown_json.setdefault(
            "hard_gate",
            {
                "passed": bool(payload.get("hard_gate_passed", False)),
                "reasons": list(payload.get("hard_gate_reasons") or []),
            },
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scores (
                        lead_id, base_score, thesis_fit, evidence_strength,
                        business_score, team_score, funding_score, traction_score,
                        market_score, thesis_fit_score, evidence_score, raw_score,
                        confidence_multiplier, boost_score, penalty_score, final_score, score_reason,
                        recommendation_band, recommendation_reason, thesis_fit_breakdown_json,
                        matched_policy_rules_json, policy_version, score_breakdown_json, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lead_id) DO UPDATE SET
                        base_score = EXCLUDED.base_score,
                        thesis_fit = EXCLUDED.thesis_fit,
                        evidence_strength = EXCLUDED.evidence_strength,
                        business_score = EXCLUDED.business_score,
                        team_score = EXCLUDED.team_score,
                        funding_score = EXCLUDED.funding_score,
                        traction_score = EXCLUDED.traction_score,
                        market_score = EXCLUDED.market_score,
                        thesis_fit_score = EXCLUDED.thesis_fit_score,
                        evidence_score = EXCLUDED.evidence_score,
                        raw_score = EXCLUDED.raw_score,
                        confidence_multiplier = EXCLUDED.confidence_multiplier,
                        boost_score = EXCLUDED.boost_score,
                        penalty_score = EXCLUDED.penalty_score,
                        final_score = EXCLUDED.final_score,
                        score_reason = EXCLUDED.score_reason,
                        recommendation_band = EXCLUDED.recommendation_band,
                        recommendation_reason = EXCLUDED.recommendation_reason,
                        thesis_fit_breakdown_json = EXCLUDED.thesis_fit_breakdown_json,
                        matched_policy_rules_json = EXCLUDED.matched_policy_rules_json,
                        policy_version = EXCLUDED.policy_version,
                        score_breakdown_json = EXCLUDED.score_breakdown_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        lead_id,
                        base_score,
                        thesis_fit,
                        evidence_strength,
                        business_score,
                        team_score,
                        funding_score,
                        traction_score,
                        market_score,
                        thesis_fit_score,
                        evidence_score,
                        raw_score,
                        confidence_multiplier,
                        boost_score,
                        penalty_score,
                        final_score,
                        recommendation_reason,
                        recommendation_band,
                        recommendation_reason,
                        json.dumps(thesis_fit_breakdown, ensure_ascii=False),
                        json.dumps(matched_policy_rules, ensure_ascii=False),
                        policy_version,
                        json.dumps(score_breakdown_json, ensure_ascii=False),
                        now,
                    ),
                )
                cur.execute("UPDATE leads SET stage = 'scoring' WHERE id = %s", (lead_id,))

    def get_recommendations(self, min_score: float, limit: int = 20) -> list[dict[str, Any]]:
        effective_min_score = max(float(min_score), 82.0)
        scoring_ready_sql = self._scoring_ready_condition_sql(lead_alias="l", dd_alias="d")
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        l.id AS lead_id,
                        l.company_name,
                        l.sources,
                        l.raw_title,
                        l.candidate_name,
                        l.normalized_name,
                        l.entity_type,
                        l.official_domain,
                        l.verification_status,
                        l.reject_reason,
                        d.dd_status,
                        COALESCE(d.business_summary, '') AS business_summary,
                        s.business_score,
                        s.team_score,
                        s.funding_score,
                        s.traction_score,
                        s.market_score,
                        s.thesis_fit_score,
                        s.evidence_score,
                        s.raw_score,
                        s.confidence_multiplier,
                        s.boost_score,
                        s.penalty_score,
                        s.final_score,
                        CASE
                            WHEN COALESCE(l.verification_status, 'pending_review') = 'likely_company' THEN 'medium'
                            ELSE 'high'
                        END AS confidence,
                        CASE
                            WHEN COALESCE(l.verification_status, 'pending_review') = 'likely_company' THEN 1
                            ELSE 0
                        END AS needs_human_review,
                        s.score_reason,
                        s.recommendation_band,
                        s.recommendation_reason,
                        s.thesis_fit_breakdown_json,
                        s.matched_policy_rules_json,
                        s.policy_version,
                        s.score_breakdown_json
                    FROM scores s
                    INNER JOIN leads l ON l.id = s.lead_id
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE s.final_score >= %s
                      AND s.recommendation_band IN ('Strong Recommend', 'Recommend')
                      AND COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                      AND COALESCE(d.source_hits, 0) >= 2
                    ORDER BY s.final_score DESC, s.updated_at DESC
                    LIMIT %s
                    """,
                    (effective_min_score, limit),
                )
                rows = list(cur.fetchall())
                return [self._with_display_name(row) for row in rows if self._lead_is_active_company(row)]

    def update_lead_status(self, lead_id: int, status: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE leads SET status = %s WHERE id = %s", (status, lead_id))

    def add_feedback(
        self,
        verdict: str,
        feedback_type: str,
        content: str,
        lead_id: int | None = None,
    ) -> int:
        now = utc_now()
        verdict = normalize_text_content(verdict)
        feedback_type = normalize_text_content(feedback_type)
        content = normalize_text_content(content)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO feedback (lead_id, verdict, feedback_type, content, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (lead_id, verdict, feedback_type, content, now),
                )
                row = cur.fetchone()
                if row and "id" in row.keys():
                    return int(row["id"])
                return int(getattr(cur, "lastrowid", 0) or 0)

    def list_recent_feedback(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM feedback ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                return list(cur.fetchall())

    def get_scoring_policy(self, policy_key: str = "default") -> dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM scoring_policies
                    WHERE policy_key = %s
                    ORDER BY version DESC, id DESC
                    LIMIT 1
                    """,
                    (policy_key,),
                )
                row = cur.fetchone()
                if row:
                    return self._scoring_policy_row_to_dict(row)

        default_policy = ScoringPolicy.default(policy_key=policy_key).to_dict()
        self._save_scoring_policy(default_policy, event=None)
        return default_policy

    def list_scoring_policy_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM scoring_policy_events
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = list(cur.fetchall())
                parsed_rows: list[dict[str, Any]] = []
                for row in rows:
                    row_dict = dict(row)
                    row_dict["change_json"] = self._json_loads(row_dict.get("change_json"), default={})
                    parsed_rows.append(row_dict)
                return parsed_rows

    def update_scoring_policy_from_feedback(self, feedback: dict[str, Any]) -> dict[str, Any]:
        current_policy = ScoringPolicy.from_dict(self.get_scoring_policy())
        updated_policy, event = current_policy.apply_feedback(feedback)
        self._save_scoring_policy(updated_policy.to_dict(), event=event)
        return {
            "ok": True,
            "policy": updated_policy.to_dict(),
            "event": event,
        }

    def _save_scoring_policy(self, policy: dict[str, Any], event: dict[str, Any] | None) -> None:
        payload = dict(policy or {})
        weights_json = json.dumps(payload.get("weights") or {}, ensure_ascii=False)
        boost_rules_json = json.dumps(payload.get("boost_rules") or [], ensure_ascii=False)
        penalty_rules_json = json.dumps(payload.get("penalty_rules") or [], ensure_ascii=False)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scoring_policies (
                        policy_key, version, weights_json, boost_rules_json, penalty_rules_json,
                        source_feedback_id, source_feedback_type, source_verdict, source_scope,
                        source_scope_key, source_lead_id, source_company_key, source_content,
                        change_summary, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (policy_key) DO UPDATE SET
                        version = EXCLUDED.version,
                        weights_json = EXCLUDED.weights_json,
                        boost_rules_json = EXCLUDED.boost_rules_json,
                        penalty_rules_json = EXCLUDED.penalty_rules_json,
                        source_feedback_id = EXCLUDED.source_feedback_id,
                        source_feedback_type = EXCLUDED.source_feedback_type,
                        source_verdict = EXCLUDED.source_verdict,
                        source_scope = EXCLUDED.source_scope,
                        source_scope_key = EXCLUDED.source_scope_key,
                        source_lead_id = EXCLUDED.source_lead_id,
                        source_company_key = EXCLUDED.source_company_key,
                        source_content = EXCLUDED.source_content,
                        change_summary = EXCLUDED.change_summary,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        str(payload.get("policy_key") or "default"),
                        int(payload.get("version") or 1),
                        weights_json,
                        boost_rules_json,
                        penalty_rules_json,
                        payload.get("source_feedback_id"),
                        str(payload.get("source_feedback_type") or ""),
                        str(payload.get("source_verdict") or ""),
                        str(payload.get("source_scope") or ""),
                        str(payload.get("source_scope_key") or ""),
                        payload.get("source_lead_id"),
                        str(payload.get("source_company_key") or ""),
                        str(payload.get("source_content") or ""),
                        str(payload.get("change_summary") or ""),
                        str(payload.get("updated_at") or utc_now()),
                    ),
                )
                if event:
                    cur.execute(
                        """
                        INSERT INTO scoring_policy_events (
                            policy_key, version, feedback_id, lead_id, company_key, company_name,
                            normalized_name, official_domain, feedback_type, verdict, scope,
                            scope_key, content, change_json, change_summary, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(event.get("policy_key") or payload.get("policy_key") or "default"),
                            int(event.get("version") or payload.get("version") or 1),
                            event.get("feedback_id"),
                            event.get("lead_id"),
                            str(event.get("company_key") or ""),
                            str(event.get("company_name") or ""),
                            str(event.get("normalized_name") or ""),
                            str(event.get("official_domain") or ""),
                            str(event.get("feedback_type") or ""),
                            str(event.get("verdict") or ""),
                            str(event.get("scope") or ""),
                            str(event.get("scope_key") or ""),
                            str(event.get("content") or ""),
                            json.dumps(event.get("change_json") or {}, ensure_ascii=False),
                            str(event.get("change_summary") or ""),
                            str(event.get("created_at") or utc_now()),
                        ),
                    )

    def _scoring_policy_row_to_dict(self, row: Any) -> dict[str, Any]:
        row_dict = dict(row)
        return {
            "policy_key": row_dict.get("policy_key", "default"),
            "version": int(row_dict.get("version") or 1),
            "weights": self._json_loads(row_dict.get("weights_json"), default={}),
            "boost_rules": self._json_loads(row_dict.get("boost_rules_json"), default=[]),
            "penalty_rules": self._json_loads(row_dict.get("penalty_rules_json"), default=[]),
            "updated_at": row_dict.get("updated_at", ""),
            "source_feedback_id": row_dict.get("source_feedback_id"),
            "source_feedback_type": row_dict.get("source_feedback_type", ""),
            "source_verdict": row_dict.get("source_verdict", ""),
            "source_scope": row_dict.get("source_scope", ""),
            "source_scope_key": row_dict.get("source_scope_key", ""),
            "source_lead_id": row_dict.get("source_lead_id"),
            "source_company_key": row_dict.get("source_company_key", ""),
            "source_content": row_dict.get("source_content", ""),
            "change_summary": row_dict.get("change_summary", ""),
        }

    def add_dd_feedback_memory(self, feedback: dict[str, Any]) -> int:
        now = utc_now()
        payload = dict(feedback or {})
        parsed = payload.get("parsed") or {}
        if not isinstance(parsed, (dict, list)):
            parsed = {}

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dd_feedback_memory (
                        scope, scope_key, lead_id, company_key, company_name, normalized_name,
                        official_domain, dimension, feedback_kind, content, parsed_json,
                        source_question_id, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        str(payload.get("scope") or "global"),
                        str(payload.get("scope_key") or "global"),
                        payload.get("lead_id"),
                        str(payload.get("company_key") or ""),
                        str(payload.get("company_name") or ""),
                        str(payload.get("normalized_name") or ""),
                        str(payload.get("official_domain") or ""),
                        str(payload.get("dimension") or "entity"),
                        str(payload.get("feedback_kind") or "note"),
                        str(payload.get("content") or ""),
                        json.dumps(parsed, ensure_ascii=False),
                        payload.get("source_question_id"),
                        now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row["id"])

    def list_dd_feedback_memory(
        self,
        *,
        lead_id: int | None = None,
        company_key: str = "",
        scope: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if scope:
            clauses.append("scope = %s")
            params.append(scope)
        if lead_id is not None:
            clauses.append("lead_id = %s")
            params.append(lead_id)
        if company_key:
            clauses.append("company_key = %s")
            params.append(company_key)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM dd_feedback_memory
                    {where_sql}
                    ORDER BY updated_at DESC, id DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = list(cur.fetchall())
                parsed_rows: list[dict[str, Any]] = []
                for row in rows:
                    row_dict = dict(row)
                    row_dict["parsed_json"] = self._json_loads(row_dict.get("parsed_json"), default={})
                    parsed_rows.append(row_dict)
                return parsed_rows

    def touch_leads_for_dd_feedback(
        self,
        *,
        scope: str,
        lead_id: int | None = None,
        company_key: str = "",
    ) -> int:
        now = utc_now()
        updated = 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                if scope == "lead" and lead_id:
                    cur.execute("UPDATE leads SET last_seen_at = %s WHERE id = %s", (now, lead_id))
                    updated = int(cur.rowcount or 0)
                elif scope == "company" and company_key:
                    cur.execute(
                        """
                        UPDATE leads
                        SET last_seen_at = %s
                        WHERE company_key = %s
                           OR normalized_name = %s
                           OR candidate_name = %s
                        """,
                        (now, company_key, company_key, company_key),
                    )
                    updated = int(cur.rowcount or 0)
                else:
                    cur.execute(
                        """
                        UPDATE leads
                        SET last_seen_at = %s
                        WHERE COALESCE(verification_status, 'pending_review') = 'verified'
                          AND COALESCE(entity_type, 'unknown') = 'company'
                        """,
                        (now,),
                    )
                    updated = int(cur.rowcount or 0)
        return updated

    def add_dd_question(self, question: dict[str, Any]) -> int:
        now = utc_now()
        payload = dict(question or {})
        missing_fields = normalize_json_payload(payload.get("missing_fields") or [])
        details = normalize_json_payload(payload.get("details") or {})
        dedupe_key = str(payload.get("dedupe_key") or "")
        if not dedupe_key:
            dedupe_key = self._dd_question_dedupe_key(
                int(payload.get("lead_id") or 0),
                str(payload.get("dimension") or "entity"),
                str(payload.get("question_type") or "missing_fields"),
                str(payload.get("prompt") or ""),
            )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dd_questions (
                        dedupe_key, lead_id, company_key, company_name, normalized_name,
                        official_domain, scope, scope_key, dimension, question_type, prompt, missing_fields,
                        details_json, status, answer_text, answer_feedback_id, published_at,
                        resolved_at, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dedupe_key) DO UPDATE SET
                        lead_id = EXCLUDED.lead_id,
                        company_key = EXCLUDED.company_key,
                        company_name = EXCLUDED.company_name,
                        normalized_name = EXCLUDED.normalized_name,
                        official_domain = EXCLUDED.official_domain,
                        scope = EXCLUDED.scope,
                        scope_key = EXCLUDED.scope_key,
                        dimension = EXCLUDED.dimension,
                        question_type = EXCLUDED.question_type,
                        prompt = EXCLUDED.prompt,
                        missing_fields = EXCLUDED.missing_fields,
                        details_json = EXCLUDED.details_json,
                        status = CASE
                            WHEN dd_questions.status = 'resolved' THEN dd_questions.status
                            ELSE EXCLUDED.status
                        END,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (
                        dedupe_key,
                        payload.get("lead_id"),
                        str(payload.get("company_key") or ""),
                        str(payload.get("company_name") or ""),
                        str(payload.get("normalized_name") or ""),
                        str(payload.get("official_domain") or ""),
                        str(payload.get("scope") or "lead"),
                        str(payload.get("scope_key") or ""),
                        str(payload.get("dimension") or "entity"),
                        str(payload.get("question_type") or "missing_fields"),
                        str(payload.get("prompt") or ""),
                        json.dumps(missing_fields, ensure_ascii=False),
                        json.dumps(details, ensure_ascii=False),
                        str(payload.get("status") or "open"),
                        str(payload.get("answer_text") or ""),
                        payload.get("answer_feedback_id"),
                        payload.get("published_at") or None,
                        payload.get("resolved_at") or None,
                        payload.get("created_at") or now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row["id"])

    def list_dd_questions(
        self,
        *,
        lead_id: int | None = None,
        company_key: str = "",
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if lead_id is not None:
            clauses.append("lead_id = %s")
            params.append(lead_id)
        if company_key:
            clauses.append("company_key = %s")
            params.append(company_key)
        if status:
            clauses.append("status = %s")
            params.append(status)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM dd_questions
                    {where_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = list(cur.fetchall())
                parsed_rows: list[dict[str, Any]] = []
                for row in rows:
                    row_dict = dict(row)
                    row_dict["missing_fields"] = self._json_loads(row_dict.get("missing_fields"), default=[])
                    row_dict["details_json"] = self._json_loads(row_dict.get("details_json"), default={})
                    parsed_rows.append(row_dict)
                return parsed_rows

    def get_dd_question(self, question_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM dd_questions WHERE id = %s", (question_id,))
                row = cur.fetchone()
                if not row:
                    return None
                result = dict(row)
                result["missing_fields"] = self._json_loads(result.get("missing_fields"), default=[])
                result["details_json"] = self._json_loads(result.get("details_json"), default={})
                return result

    def mark_dd_questions_published(self, question_ids: list[int]) -> int:
        if not question_ids:
            return 0
        now = utc_now()
        placeholders = ",".join(["%s"] * len(question_ids))
        sql = f"UPDATE dd_questions SET published_at = %s, updated_at = %s WHERE id IN ({placeholders}) AND published_at IS NULL"
        params = [now, now, *question_ids]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return int(cur.rowcount or 0)

    def resolve_dd_question(self, *, question_id: int, answer_text: str, answer_feedback_id: int | None = None) -> int:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE dd_questions
                    SET status = 'resolved',
                        answer_text = %s,
                        answer_feedback_id = %s,
                        resolved_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (answer_text, answer_feedback_id, now, now, question_id),
                )
                return int(cur.rowcount or 0)

    def _dd_question_dedupe_key(self, lead_id: int, dimension: str, question_type: str, prompt: str) -> str:
        payload = f"{lead_id}|{dimension}|{question_type}|{prompt}".encode("utf-8")
        return sha1(payload).hexdigest()

    def set_long_memory(self, memory_key: str, memory_value: dict[str, Any]) -> None:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO memory_long (memory_key, memory_value, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (memory_key) DO UPDATE SET
                        memory_value = EXCLUDED.memory_value,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (memory_key, json.dumps(memory_value, ensure_ascii=False), now),
                )

    def get_long_memory(self, memory_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT memory_value FROM memory_long WHERE memory_key = %s", (memory_key,))
                row = cur.fetchone()
                if not row:
                    return None
                return self._json_loads(row["memory_value"], default={})

    def set_short_memory(self, memory_date: str, strategy_value: dict[str, Any]) -> None:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO memory_short (memory_date, strategy_value, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (memory_date) DO UPDATE SET
                        strategy_value = EXCLUDED.strategy_value,
                        created_at = EXCLUDED.created_at
                    """,
                    (memory_date, json.dumps(strategy_value, ensure_ascii=False), now),
                )

    def get_latest_short_memory(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT strategy_value FROM memory_short ORDER BY memory_date DESC LIMIT 1")
                row = cur.fetchone()
                if not row:
                    return None
                return self._json_loads(row["strategy_value"], default={})

    def get_previous_short_memory(self, *, exclude_date: str = "") -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if exclude_date:
                    cur.execute(
                        """
                        SELECT strategy_value
                        FROM memory_short
                        WHERE memory_date <> %s
                        ORDER BY memory_date DESC
                        LIMIT 1
                        """,
                        (exclude_date,),
                    )
                else:
                    cur.execute(
                        "SELECT strategy_value FROM memory_short ORDER BY memory_date DESC LIMIT 1"
                    )
                row = cur.fetchone()
                if not row:
                    return None
                return self._json_loads(row["strategy_value"], default={})

    def list_recent_short_memories(self, *, days: int = 7, limit: int = 30) -> list[dict[str, Any]]:
        cutoff = utc_now().date().fromordinal(utc_now().date().toordinal() - max(days - 1, 0)).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT memory_date, strategy_value, created_at
                    FROM memory_short
                    WHERE memory_date >= %s
                    ORDER BY memory_date DESC
                    LIMIT %s
                    """,
                    (cutoff, limit),
                )
                rows = []
                for row in cur.fetchall():
                    row_dict = dict(row)
                    row_dict["strategy_value"] = self._json_loads(row_dict.get("strategy_value"), default={})
                    rows.append(row_dict)
                return rows

    def add_planner_feedback_memory(self, payload: dict[str, Any]) -> int:
        now = utc_now()
        source_feedback_id = payload.get("source_feedback_id")
        feedback_type = normalize_text_content(payload.get("feedback_type") or "")
        target = normalize_text_content(payload.get("target") or "")
        value = normalize_text_content(payload.get("value") or "")
        status = normalize_text_content(payload.get("status") or "active") or "active"
        created_at = payload.get("created_at") or now
        updated_at = payload.get("updated_at") or now

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO planner_feedback_memory (
                        source_feedback_id, feedback_type, target, value, status, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (source_feedback_id, feedback_type, target, value, status, created_at, updated_at),
                )
                row = cur.fetchone()
                if row and "id" in row.keys():
                    return int(row["id"])
                return int(getattr(cur, "lastrowid", 0) or 0)

    def add_planner_feedback_event(self, payload: dict[str, Any]) -> int:
        now = utc_now()
        event = PlannerFeedbackEvent(
            feedback_type=normalize_text_content(payload.get("feedback_type") or ""),
            target=normalize_text_content(payload.get("target") or ""),
            value=normalize_text_content(payload.get("value") or ""),
            status=normalize_text_content(payload.get("status") or "active") or "active",
            source_feedback_id=payload.get("source_feedback_id"),
            merged=bool(payload.get("merged", False)),
            merge_summary=normalize_text_content(payload.get("merge_summary") or ""),
            metadata=normalize_json_payload(payload.get("metadata") or {}),
            created_at=str(payload.get("created_at") or now),
            updated_at=str(payload.get("updated_at") or now),
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO planner_feedback_events (
                        source_feedback_id, feedback_type, target, value, status, merged, merge_summary, metadata_json, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        event.source_feedback_id,
                        event.feedback_type,
                        event.target,
                        event.value,
                        event.status,
                        1 if event.merged else 0,
                        event.merge_summary,
                        json.dumps(event.metadata, ensure_ascii=False),
                        event.created_at,
                        event.updated_at,
                    ),
                )
                row = cur.fetchone()
                if row and "id" in row.keys():
                    return int(row["id"])
                return int(getattr(cur, "lastrowid", 0) or 0)

    def has_planner_feedback_source(self, source_feedback_id: int) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM planner_feedback_memory WHERE source_feedback_id = %s LIMIT 1",
                    (source_feedback_id,),
                )
                return cur.fetchone() is not None

    def has_planner_feedback_event_source(self, source_feedback_id: int) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM planner_feedback_events WHERE source_feedback_id = %s LIMIT 1",
                    (source_feedback_id,),
                )
                return cur.fetchone() is not None

    def list_planner_feedback_memory(self, *, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if status:
                    cur.execute(
                        """
                        SELECT *
                        FROM planner_feedback_memory
                        WHERE status = %s
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (status, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM planner_feedback_memory
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (limit,),
                )
                return [dict(row) for row in cur.fetchall()]

    def replace_planner_feedback_memory(self, items: list[dict[str, Any]]) -> None:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM planner_feedback_memory")
                for payload in items:
                    cur.execute(
                        """
                        INSERT INTO planner_feedback_memory (
                            source_feedback_id, feedback_type, target, value, status, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            payload.get("source_feedback_id"),
                            normalize_text_content(payload.get("feedback_type") or ""),
                            normalize_text_content(payload.get("target") or ""),
                            normalize_text_content(payload.get("value") or ""),
                            normalize_text_content(payload.get("status") or "active") or "active",
                            payload.get("created_at") or now,
                            payload.get("updated_at") or now,
                        ),
                    )

    def list_planner_feedback_events(self, *, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if status:
                    cur.execute(
                        """
                        SELECT *
                        FROM planner_feedback_events
                        WHERE status = %s
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (status, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM planner_feedback_events
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                rows = []
                for row in cur.fetchall():
                    item = dict(row)
                    item["merged"] = bool(item.get("merged"))
                    item["metadata_json"] = self._json_loads(item.get("metadata_json"), default={})
                    rows.append(item)
                return rows

    def mark_planner_feedback_event_merged(self, event_id: int, *, merge_summary: str = "") -> None:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE planner_feedback_events
                    SET merged = %s,
                        merge_summary = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (1, normalize_text_content(merge_summary), now, event_id),
                )

    def list_recent_signals(self, *, days: int = 7, limit: int = 300) -> list[dict[str, Any]]:
        cutoff = utc_now()
        if days > 0:
            from datetime import timedelta
            cutoff = cutoff - timedelta(days=days)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT lead_id, source, query, title, snippet, url, fetched_at, raw_json
                    FROM signals
                    WHERE fetched_at >= %s
                    ORDER BY fetched_at DESC, id DESC
                    LIMIT %s
                    """,
                    (cutoff, limit),
                )
                rows = []
                for row in cur.fetchall():
                    row_dict = dict(row)
                    row_dict["raw_json"] = self._json_loads(row_dict.get("raw_json"), default={})
                    rows.append(row_dict)
                return rows

    def add_planner_compaction(self, compaction_value: dict[str, Any]) -> int:
        now = utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO planner_compactions (compaction_value, created_at)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (json.dumps(compaction_value, ensure_ascii=False), now),
                )
                row = cur.fetchone()
                if row and "id" in row.keys():
                    return int(row["id"])
                return int(getattr(cur, "lastrowid", 0) or 0)

    def add_planner_compaction_run(self, payload: dict[str, Any]) -> int:
        now = utc_now()
        run = PlannerCompactionRun(
            promoted_themes=list(payload.get("promoted_themes", [])),
            decayed_themes=list(payload.get("decayed_themes", [])),
            merged_topics=list(payload.get("merged_topics", [])),
            archived_preferences=list(payload.get("archived_preferences", [])),
            source_policy_changes=list(payload.get("source_policy_changes", [])),
            summary=normalize_text_content(payload.get("summary") or ""),
            created_at=str(payload.get("created_at") or now),
        ).to_dict()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO planner_compaction_runs (run_value, created_at)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (json.dumps(run, ensure_ascii=False), now),
                )
                row = cur.fetchone()
                if row and "id" in row.keys():
                    return int(row["id"])
                return int(getattr(cur, "lastrowid", 0) or 0)

    def get_latest_planner_compaction(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT compaction_value
                    FROM planner_compactions
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                return self._json_loads(row["compaction_value"], default={})

    def get_latest_planner_compaction_run(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_value
                    FROM planner_compaction_runs
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                return self._json_loads(row["run_value"], default={})

    def save_outbox_event(self, event_type: str, payload: dict[str, Any]) -> None:
        now = utc_now()
        normalized_payload = normalize_json_payload(payload)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO outbox (event_type, payload, created_at) VALUES (%s, %s, %s)",
                    (event_type, json.dumps(normalized_payload, ensure_ascii=False), now),
                )

    def list_pending_outbox(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, event_type, payload, sent, created_at, sent_at
                    FROM outbox
                    WHERE sent = 0
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return list(cur.fetchall())

    def mark_outbox_sent(self, event_ids: list[int]) -> int:
        if not event_ids:
            return 0

        now = utc_now()
        placeholders = ", ".join(["%s"] * len(event_ids))
        sql = f"UPDATE outbox SET sent = 1, sent_at = %s WHERE id IN ({placeholders})"

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [now, *event_ids])
                return int(cur.rowcount or 0)

    def log_conversation_message(
        self,
        *,
        direction: str,
        source: str,
        content: str,
        session_key: str = "",
        channel_id: str = "",
        sender: str = "",
        action: str = "",
        payload: dict[str, Any] | None = None,
    ) -> int:
        now = utc_now()
        direction = normalize_text_content(direction)
        source = normalize_text_content(source)
        content = normalize_text_content(content)
        session_key = normalize_text_content(session_key)
        channel_id = normalize_text_content(channel_id)
        sender = normalize_text_content(sender)
        action = normalize_text_content(action)
        payload_json = json.dumps(normalize_json_payload(payload or {}), ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_messages (
                        direction, source, session_key, channel_id, sender,
                        content, action, payload_json, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        direction,
                        source,
                        session_key,
                        channel_id,
                        sender,
                        content,
                        action,
                        payload_json,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row["id"])

    def list_conversation_messages(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM conversation_messages
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return list(cur.fetchall())

    def list_recent_conversation_messages(
        self,
        *,
        days: int = 7,
        limit: int = 200,
        direction: str | None = None,
    ) -> list[dict[str, Any]]:
        cutoff = utc_now()
        if days > 0:
            from datetime import timedelta
            cutoff = cutoff - timedelta(days=days)
        with self._connect() as conn:
            with conn.cursor() as cur:
                if direction:
                    cur.execute(
                        """
                        SELECT *
                        FROM conversation_messages
                        WHERE created_at >= %s AND direction = %s
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s
                        """,
                        (cutoff, direction, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM conversation_messages
                        WHERE created_at >= %s
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s
                        """,
                        (cutoff, limit),
                    )
                return [dict(row) for row in cur.fetchall()]

    def list_leads(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM leads ORDER BY last_seen_at DESC LIMIT %s",
                    (limit,),
                )
                return [self._with_display_name(row) for row in cur.fetchall()]

    def get_recommendation_blocker_counts(self) -> dict[str, int]:
        eligible_statuses = self._eligible_company_statuses()
        status_placeholders = ", ".join(["%s"] * len(eligible_statuses))
        scoring_ready_sql = self._scoring_ready_condition_sql(lead_alias="l", dd_alias="d")

        def _count_from_row(row: Any) -> int:
            if not row:
                return 0
            try:
                return int(row["value"] or 0)
            except Exception:
                try:
                    return int(row[0] or 0)
                except Exception:
                    return 0

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM leads
                    WHERE COALESCE(verification_status, 'pending_review') IN ({status_placeholders})
                      AND COALESCE(entity_type, 'unknown') = 'company'
                    """,
                    eligible_statuses,
                )
                verified_company_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM dd_reports d
                    INNER JOIN leads l ON l.id = d.lead_id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                    """,
                    (),
                )
                dd_ready_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM dd_reports d
                    INNER JOIN leads l ON l.id = d.lead_id
                    WHERE COALESCE(l.verification_status, 'pending_review') IN ({status_placeholders})
                      AND COALESCE(l.entity_type, 'unknown') = 'company'
                      AND COALESCE(d.dd_status, 'dd_pending_review') = 'dd_waiting_human'
                    """,
                    eligible_statuses,
                )
                waiting_human_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM scores s
                    INNER JOIN leads l ON l.id = s.lead_id
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                    """,
                    (),
                )
                scored_ready_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM scores s
                    INNER JOIN leads l ON l.id = s.lead_id
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                      AND COALESCE(d.source_hits, 0) >= 2
                      AND s.recommendation_band = 'Watchlist'
                    """,
                    (),
                )
                watchlist_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM scores s
                    INNER JOIN leads l ON l.id = s.lead_id
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                      AND COALESCE(d.source_hits, 0) < 2
                    """,
                    (),
                )
                hard_gate_blocked_count = _count_from_row(cur.fetchone())

                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM scores s
                    INNER JOIN leads l ON l.id = s.lead_id
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE COALESCE(l.entity_type, 'unknown') = 'company'
                      AND {scoring_ready_sql}
                      AND COALESCE(d.source_hits, 0) >= 2
                      AND s.recommendation_band IN ('Strong Recommend', 'Recommend')
                    """,
                    (),
                )
                push_ready_count = _count_from_row(cur.fetchone())

        return {
            "verified_company_count": verified_company_count,
            "dd_ready_count": dd_ready_count,
            "waiting_human_count": waiting_human_count,
            "scored_ready_count": scored_ready_count,
            "watchlist_count": watchlist_count,
            "hard_gate_blocked_count": hard_gate_blocked_count,
            "push_ready_count": push_ready_count,
        }

    def find_leads_by_company_query(self, company_query: str, limit: int = 5) -> list[dict[str, Any]]:
        query = str(company_query or "").strip()
        if not query:
            return []

        pattern = f"%{query.lower()}%"
        company_key = self._company_key_from_name(query)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        company_name,
                        company_key,
                        status,
                        stage,
                        description,
                        thesis_tags,
                        sources,
                        raw_title,
                        candidate_name,
                        normalized_name,
                        entity_type,
                        official_domain,
                        verification_status,
                        verification_score,
                        reject_reason,
                        resolution_json,
                        first_seen_at,
                        last_seen_at
                    FROM leads
                    WHERE company_key = %s
                       OR LOWER(COALESCE(company_name, '')) LIKE %s
                       OR LOWER(COALESCE(normalized_name, '')) LIKE %s
                       OR LOWER(COALESCE(candidate_name, '')) LIKE %s
                       OR LOWER(COALESCE(raw_title, '')) LIKE %s
                    ORDER BY
                        CASE
                            WHEN company_key = %s THEN 0
                            WHEN COALESCE(verification_status, 'pending_review') = 'verified'
                                 AND COALESCE(entity_type, 'unknown') = 'company' THEN 1
                            WHEN LOWER(COALESCE(normalized_name, '')) = LOWER(%s) THEN 2
                            WHEN LOWER(COALESCE(company_name, '')) = LOWER(%s) THEN 3
                            ELSE 4
                        END,
                        last_seen_at DESC
                    LIMIT %s
                    """,
                    (company_key, pattern, pattern, pattern, pattern, company_key, query, query, limit),
                )
                return [self._with_display_name(row) for row in cur.fetchall()]

    def get_dd_report_for_lead(self, lead_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.id AS lead_id,
                        l.company_name,
                        l.company_key,
                        l.status,
                        l.stage,
                        l.description,
                        l.thesis_tags,
                        l.sources,
                        l.raw_title,
                        l.candidate_name,
                        l.normalized_name,
                        l.entity_type,
                        l.official_domain,
                        l.verification_status,
                        l.verification_score,
                        l.reject_reason,
                        l.resolution_json,
                        d.business_summary,
                        d.team_summary,
                        d.funding_summary,
                        d.traction_summary,
                        d.industry_position,
                        d.business_profile_json,
                        d.team_profile_json,
                        d.funding_profile_json,
                        d.traction_profile_json,
                        d.market_position_json,
                        d.dd_overall_json,
                        d.questions_json,
                        d.source_hits,
                        d.completeness_score,
                        d.dd_status,
                        d.evidence_json,
                        d.updated_at AS dd_updated_at
                    FROM leads l
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    WHERE l.id = %s
                    """,
                    (lead_id,),
                )
                row = cur.fetchone()
                return self._hydrate_dd_record(dict(row)) if row else None

    def get_company_analysis_for_lead(self, lead_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.id AS lead_id,
                        l.company_name,
                        l.company_key,
                        l.status,
                        l.stage,
                        l.description,
                        l.thesis_tags,
                        l.sources,
                        l.raw_title,
                        l.candidate_name,
                        l.normalized_name,
                        l.entity_type,
                        l.official_domain,
                        l.verification_status,
                        l.verification_score,
                        l.reject_reason,
                        l.resolution_json,
                        l.first_seen_at,
                        l.last_seen_at,
                        d.business_summary,
                        d.team_summary,
                        d.funding_summary,
                        d.traction_summary,
                        d.industry_position,
                        d.business_profile_json,
                        d.team_profile_json,
                        d.funding_profile_json,
                        d.traction_profile_json,
                        d.market_position_json,
                        d.dd_overall_json,
                        d.questions_json,
                        d.source_hits,
                        d.completeness_score,
                        d.dd_status,
                        d.evidence_json,
                        d.updated_at AS dd_updated_at,
                        s.business_score,
                        s.team_score,
                        s.funding_score,
                        s.traction_score,
                        s.market_score,
                        s.thesis_fit_score,
                        s.evidence_score,
                        s.raw_score,
                        s.confidence_multiplier,
                        s.boost_score,
                        s.penalty_score,
                        s.base_score,
                        s.thesis_fit,
                        s.evidence_strength,
                        s.final_score,
                        s.score_reason,
                        s.recommendation_band,
                        s.recommendation_reason,
                        s.thesis_fit_breakdown_json,
                        s.matched_policy_rules_json,
                        s.policy_version,
                        s.score_breakdown_json,
                        s.updated_at AS score_updated_at
                    FROM leads l
                    LEFT JOIN dd_reports d ON d.lead_id = l.id
                    LEFT JOIN scores s ON s.lead_id = l.id
                    WHERE l.id = %s
                    """,
                    (lead_id,),
                )
                row = cur.fetchone()
                return self._hydrate_dd_record(dict(row)) if row else None
