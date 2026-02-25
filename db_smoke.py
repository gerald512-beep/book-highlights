import os, psycopg
from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.local", override=True)

DB = os.getenv("DATABASE_URL")
assert DB


def run():
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # book
        cur.execute(
            """
          insert into books (book_id, title, author_primary, isbn13, publisher, pub_year, category, cover_url)
          values (%s,%s,%s,%s,%s,%s,%s,%s)
          on conflict (book_id) do nothing
        """,
            (
                "atomic-habits-9780735211292",
                "Atomic Habits",
                "James Clear",
                "9780735211292",
                "Avery",
                2018,
                "Advice, How-To & Misc",
                "https://images/atomic.jpg",
            ),
        )

        # quote (approved + commentary)
        cur.execute(
            """
          insert into quotes (book_id, quote_text, source_name, source_url, my_commentary, is_approved)
          values (%s,%s,%s,%s,%s,true)
          returning quote_id
        """,
            (
                "atomic-habits-9780735211292",
                "You do not rise to the level of your goals. You fall to the level of your systems.",
                "Goodreads (search)",
                "https://www.goodreads.com/quotes/search?q=Atomic+Habits",
                "My take: systems make consistency cheap; goals without systems are cosplay.",
            ),
        )
        quote_id = cur.fetchone()[0]

        # publication
        cur.execute(
            """
          insert into publications (book_id, intro_text, outro_text, affiliate_url, status)
          values (%s,%s,%s,%s,'draft')
          returning publication_id
        """,
            (
                "atomic-habits-9780735211292",
                "Tiny wins compound. Two lines everyone underlines:",
                "Full context and links in bio.",
                "https://your.aff.link?utm_source=instagram&utm_medium=bio&utm_campaign=atomic",
            ),
        )
        pub_id = cur.fetchone()[0]

        # attach quote at position 1
        cur.execute(
            """
          insert into publication_quotes (publication_id, quote_id, position)
          values (%s,%s,1)
        """,
            (pub_id, quote_id),
        )

        conn.commit()

    print("Smoke test inserted rows. ✅")


if __name__ == "__main__":
    run()
