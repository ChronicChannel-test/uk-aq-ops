# Cross-repo map: uk-aq-history

## Main repo
- `CIC-test-uk-aq-ingest` is the main repo for this project and the default starting point for cross-repo tasks.

## Purpose
This repo is intended for historical backfills, batch processing, and analysis scripts that work on the UK AQ dataset. It is intentionally minimal for now.

## Repo structure (top-level)
- `README.md`: Minimal repo description.

(Additional folders or scripts not found in the current checkout; confirm if/when this changes.)

## How this repo connects to the others
- **Schema source**: `uk-aq-schema` defines the database structure used by historical jobs.
- **Ingest repo**: `CIC-test-uk-aq-ingest` handles live ingestion and Edge Functions.
- **Change flow**: schema changes in `uk-aq-schema` can require updating history scripts here (confirm).

## Setup & run (lightweight)
### Required env vars (names only; discoverable in code)
No env vars found in this repo. If history scripts are added later, document their env vars here (confirm).

### Commands
No scripted commands were found in this repo (confirm).

## Where to start
- `README.md`
- (Confirm) primary entry scripts or modules if/when added.

## Conventions
- Match connector codes and schema naming from the ingest/schema repos.
- Naming conventions (project-wide) live in the ingest repo: `../CIC-test-uk-aq-ingest/AGENTS.md`.

## Permissions (REQUIRED)
- The agent may edit any files without asking for permission, except files under any `/archive` directory.

## Links
- Existing README: `README.md`
- Ingest repo (sibling): `../CIC-test-uk-aq-ingest`
- Naming conventions (ingest repo): `../CIC-test-uk-aq-ingest/AGENTS.md`
- Schema repo (sibling): `../CIC-test-uk-aq-schema/uk-aq-schema`

## WORKING STYLE (IMPORTANT)

REQUIRED OUTPUT FORMAT

Summary (2–5 bullets)
Files changed (paths)
Implementation details (short, specific)
Supabase steps (instructions only,)
Verification checklist (clear pass/fail)

Planning requirement:
- For plan proposals, always assess both egress and database-size effects. Include those effects in options/pros/cons and reference them in the final recommendation.
