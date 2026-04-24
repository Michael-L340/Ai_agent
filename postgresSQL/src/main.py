from __future__ import annotations

import argparse
from typing import Sequence

from .db import open_connection

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS notes (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def ensure_table() -> None:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(TABLE_SQL)
    print("Table ready: notes")


def create_note(title: str, body: str) -> None:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notes (title, body)
                VALUES (%s, %s)
                RETURNING id, title, body, created_at, updated_at
                """,
                (title, body),
            )
            row = cur.fetchone()
    print(
        f"Created note #{row['id']}: {row['title']} "
        f"({row['created_at']})"
    )


def list_notes() -> None:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, body, created_at, updated_at
                FROM notes
                ORDER BY id
                """
            )
            rows = cur.fetchall()

    if not rows:
        print("No notes found.")
        return

    for row in rows:
        print(
            f"#{row['id']} | {row['title']} | {row['body']} "
            f"| created={row['created_at']} | updated={row['updated_at']}"
        )


def update_note(note_id: int, title: str, body: str) -> None:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notes
                SET title = %s,
                    body = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, title, body, created_at, updated_at
                """,
                (title, body, note_id),
            )
            row = cur.fetchone()

    if row is None:
        print(f"No note found for id={note_id}.")
        return

    print(f"Updated note #{row['id']}: {row['title']}")


def delete_note(note_id: int) -> None:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM notes
                WHERE id = %s
                RETURNING id, title
                """,
                (note_id,),
            )
            row = cur.fetchone()

    if row is None:
        print(f"No note found for id={note_id}.")
        return

    print(f"Deleted note #{row['id']}: {row['title']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Neon + PostgreSQL CRUD demo for Python."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create the notes table.")

    create_parser = subparsers.add_parser("create", help="Insert a note.")
    create_parser.add_argument("--title", required=True, help="Note title.")
    create_parser.add_argument("--body", required=True, help="Note body.")

    subparsers.add_parser("list", help="List all notes.")

    update_parser = subparsers.add_parser("update", help="Update a note.")
    update_parser.add_argument("--id", type=int, required=True, help="Note id.")
    update_parser.add_argument("--title", required=True, help="New title.")
    update_parser.add_argument("--body", required=True, help="New body.")

    delete_parser = subparsers.add_parser("delete", help="Delete a note.")
    delete_parser.add_argument("--id", type=int, required=True, help="Note id.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        ensure_table()
    elif args.command == "create":
        create_note(args.title, args.body)
    elif args.command == "list":
        list_notes()
    elif args.command == "update":
        update_note(args.id, args.title, args.body)
    elif args.command == "delete":
        delete_note(args.id)
    else:
        parser.error(f"Unknown command: {args.command}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
