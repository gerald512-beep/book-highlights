# Book Highlights Pipeline

Automates a full content pipeline for book-quote social posts:

- Harvests nonfiction bestsellers and quote candidates
- Stores books/quotes in Postgres
- Generates branded quote images + CTA images
- Generates Instagram/Facebook/Twitter caption drafts (OpenAI-backed with local fallback)
- Supports review/approval before publishing
- Publishes to X (Twitter) and tracks manual posting confirmations for LinkedIn/Facebook/Instagram
- Visit https://www.linkedin.com/company/missed-pages-books/ to see post examples

## Core Workflow

1. `bestsellers_and_quotes.py`  
   Pull NYT bestseller lists, enrich metadata with Google Books, fetch quote candidates, persist to DB, and write JSON/CSV bundles.
2. `prepublish.py`  
   Select a book, mark draft/discard quotes, render images, and create caption files under `prepublish_out/`.
3. `prepublish.py review_draft`  
   Review drafted quotes/captions, confirm or edit caption text, and approve content.
4. `publish.py`  
   Publish images to X (if credentials exist), confirm manual posting to other platforms, and mark DB publish status.

## Repository Scripts

- `main_menu.py`: small launcher for key menus
- `caption.py`: OpenAI caption helper + batch caption regeneration
- `db_bootstrap.py`: schema bootstrap
- `db_smoke.py`, `db_smoke_tests.py`: DB sanity checks
- `clean_db.py`, `clean_db_all.py`: safe reset/cleanup utilities

## Requirements

- Python 3.10+
- PostgreSQL database (`DATABASE_URL`)
- Network access for external APIs

Install dependencies:

```powershell
py -m pip install requests beautifulsoup4 python-dotenv pandas psycopg[binary] pillow openai requests-oauthlib
```

## Environment Setup

Copy env template and fill values:

```powershell
Copy-Item .env.example .env
```

Common variables:

- Required base:
  - `DATABASE_URL`
- Harvesting:
  - `NYT_API_KEY`
- Caption generation (OpenAI):
  - `OPENAI_API_KEY`
  - Optional: `OPENAI_MODEL`, `AFFILIATE_LINK`, `AFFILIATE_DISCLOSURE`
- Publishing:
  - `TWITTER_API_KEY`
  - `TWITTER_API_SECRET`
  - `TWITTER_ACCESS_TOKEN`
  - `TWITTER_ACCESS_SECRET`
  - Optional/manual workflow vars: `LINKEDIN_ACCESS_TOKEN`, `LINKEDIN_ORG_URN`

## Database Bootstrap

Initialize schema:

```powershell
py db_bootstrap.py
```

For fresh databases, add publish-status columns used by runtime scripts:

```sql
alter table books add column if not exists is_deleted boolean default false;
alter table books add column if not exists deleted_at timestamptz;
alter table quotes add column if not exists is_published boolean default false;
alter table quotes add column if not exists published_at timestamptz;
alter table generated_captions add column if not exists published_at timestamptz;
```

## Usage

Run from repo root:

```powershell
py bestsellers_and_quotes.py
py prepublish.py
py prepublish.py review_draft
py publish.py
```

Optional launcher:

```powershell
py main_menu.py
```

## Output Folders

- `prepublish_out/<book-slug>/`
  - Quote images (`*.png`)
  - CTA images (`*-cta-IG.png`, `*-cta-FB.png`)
  - Captions (`IGcaption.txt`, `FBcaption.txt`, `TWcaption.txt`)
  - Editable overrides (`IGnewcaption.txt`, `FBnewcaption.txt`, `newAFlink.txt`)
  - Optional PDF bundle (`*-images.pdf`)
- `publish_out/<book-slug>/`
  - Publish audit log (`publish-log.json`)

## Current Platform Automation Status

- X/Twitter: automated posting supported
- LinkedIn: manual confirmation flow (automation disabled in current code)
- Facebook/Instagram: manual posting + confirmation flow
