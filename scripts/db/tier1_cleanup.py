"""
Tier 1 DB cleanup - deletes junk ucsd_graph books in small batches.

Targets: no description, no embedding, not CLEANED, not EMBED_QUEUED.
Deletes reviews (by book_id and isbn13) before each batch of books.
Commits every BATCH_SIZE books to avoid Neon connection timeouts.
"""

import os
import sys
from pathlib import Path
import psycopg2

# Load .env from repo root
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

DB_URL = os.environ["DATABASE_URL_1"]
BATCH_SIZE = 1000   # small batches to stay under Neon's SSL timeout
MAX_TOTAL = 140_000  # hard cap

JUNK_FILTER = """
    source = 'ucsd_graph'
    AND metadata_embedding IS NULL
    AND review_embedding IS NULL
    AND NOT ('CLEANED'      = ANY(COALESCE(cleaning_flags, ARRAY[]::text[])))
    AND NOT ('EMBED_QUEUED' = ANY(COALESCE(cleaning_flags, ARRAY[]::text[])))
    AND synopsis IS NULL
    AND short_description IS NULL
    AND plot_summary IS NULL
"""


def run_batch(ids: list, isbn13s: list, retries: int = 5) -> tuple[int, int]:
    """Open a fresh connection, delete one batch, commit, close. Returns (books, reviews).
    Retries up to `retries` times on SSL/connection drops."""
    import time
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(DB_URL, connect_timeout=30)
            conn.autocommit = False
            cur = conn.cursor()

            cur.execute("DELETE FROM reviews WHERE book_id = ANY(%s)", (ids,))
            rev_by_id = cur.rowcount

            if isbn13s:
                cur.execute("DELETE FROM reviews WHERE isbn13 = ANY(%s)", (isbn13s,))
                rev_by_isbn = cur.rowcount
            else:
                rev_by_isbn = 0

            cur.execute("DELETE FROM books WHERE id = ANY(%s)", (ids,))
            books_deleted = cur.rowcount

            conn.commit()
            cur.close()
            conn.close()
            return books_deleted, rev_by_id + rev_by_isbn

        except psycopg2.OperationalError as e:
            print(f"  Connection error on attempt {attempt}/{retries}: {e}. Retrying...")
            sys.stdout.flush()
            time.sleep(3 * attempt)  # back off before retry

    raise RuntimeError(f"Batch failed after {retries} retries.")


def run():
    total_books = 0
    total_reviews = 0
    batch_num = 0

    while total_books < MAX_TOTAL:
        batch_limit = min(BATCH_SIZE, MAX_TOTAL - total_books)

        # Fetch IDs using a fresh short-lived connection
        conn = psycopg2.connect(DB_URL, connect_timeout=30)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, isbn13 FROM books
            WHERE {JUNK_FILTER}
            LIMIT %s
        """, (batch_limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            print("No more junk books found. Done.")
            break

        ids = [r[0] for r in rows]
        isbn13s = [r[1] for r in rows if r[1] is not None]

        books_deleted, reviews_deleted = run_batch(ids, isbn13s)

        total_books += books_deleted
        total_reviews += reviews_deleted
        batch_num += 1

        print(
            f"Batch {batch_num}: -{books_deleted} books, "
            f"-{reviews_deleted} reviews "
            f"(total: {total_books} books, {total_reviews} reviews)"
        )
        sys.stdout.flush()

    print(f"\nDone. Deleted {total_books} books and {total_reviews} reviews total.")


if __name__ == "__main__":
    run()
