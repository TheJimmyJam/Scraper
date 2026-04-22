-- ============================================================
-- Rebuild Digital Co — Universal Scraper DB Setup
-- Run this in Supabase > SQL Editor
-- ============================================================

-- 1. Add job_type + result_count to scrape_runs
--    (these columns may already exist — IF NOT EXISTS is safe to re-run)
ALTER TABLE scrape_runs
  ADD COLUMN IF NOT EXISTS job_type     text    DEFAULT 'google_maps_business',
  ADD COLUMN IF NOT EXISTS result_count integer DEFAULT 0;

-- 2. Universal scrape results table
--    One row per scraped item. Works for every job type.
--    `data` holds the full row as JSONB so nothing gets lost.
--    Common fields are promoted to real columns for easy querying.
CREATE TABLE IF NOT EXISTS scrape_results (
  id          uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  job_type    text        NOT NULL,
  session_id  uuid,                            -- references scrape_runs(id)
  data        jsonb       NOT NULL DEFAULT '{}', -- full scraped row
  name        text,                            -- item name / question / email / title
  category    text,                            -- job category label
  source_url  text,                            -- originating URL
  scraped_at  timestamptz DEFAULT now(),
  created_at  timestamptz DEFAULT now()
);

-- Indexes for fast lookups by session + job type
CREATE INDEX IF NOT EXISTS scrape_results_session_idx  ON scrape_results(session_id);
CREATE INDEX IF NOT EXISTS scrape_results_job_type_idx ON scrape_results(job_type);
CREATE INDEX IF NOT EXISTS scrape_results_category_idx ON scrape_results(category);
CREATE INDEX IF NOT EXISTS scrape_results_created_idx  ON scrape_results(created_at DESC);

-- 3. RLS — same pattern as other tables (any logged-in user can read/write)
ALTER TABLE scrape_results ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Auth users can read scrape_results"  ON scrape_results;
DROP POLICY IF EXISTS "Auth users can write scrape_results" ON scrape_results;

CREATE POLICY "Auth users can read scrape_results"
  ON scrape_results FOR SELECT TO authenticated USING (true);

CREATE POLICY "Auth users can write scrape_results"
  ON scrape_results FOR ALL TO authenticated USING (true);

-- Also allow anon role to read (needed by the React tracker app which uses the anon key)
DROP POLICY IF EXISTS "Anon can read scrape_results" ON scrape_results;
CREATE POLICY "Anon can read scrape_results"
  ON scrape_results FOR SELECT TO anon USING (true);

-- Also allow service role (used by server.py) to bypass RLS
-- (service key already bypasses RLS by default in Supabase — no policy needed)

-- ============================================================
-- Helper RPC functions (called by both the React app and server.py)
-- ============================================================

-- Returns the name of every user-created table in the public schema.
-- React app calls this via supabase.rpc('get_user_tables') to populate
-- the "Save Results To" dropdown.
CREATE OR REPLACE FUNCTION public.get_user_tables()
RETURNS TABLE(table_name text)
LANGUAGE sql SECURITY DEFINER AS $$
  SELECT table_name::text
  FROM information_schema.tables
  WHERE table_schema = 'public'
    AND table_type   = 'BASE TABLE'
  ORDER BY table_name;
$$;

-- Returns column names + types for a given table.
-- server.py calls this before inserting scraped data so it only passes
-- columns that actually exist in the target table.
CREATE OR REPLACE FUNCTION public.get_table_columns(p_table_name text)
RETURNS TABLE(column_name text, data_type text)
LANGUAGE sql SECURITY DEFINER AS $$
  SELECT column_name::text, data_type::text
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name   = p_table_name
  ORDER BY ordinal_position;
$$;

-- Grant execute to anon + authenticated so both the React app and the
-- server (service key) can call these functions.
GRANT EXECUTE ON FUNCTION public.get_user_tables()             TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_table_columns(text)       TO anon, authenticated;

-- ============================================================
-- DONE. Verify by running:
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'scrape_results';
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'scrape_runs'
--   AND column_name IN ('job_type', 'result_count');
-- ============================================================
