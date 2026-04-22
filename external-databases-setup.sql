-- ============================================================
-- Rebuild Digital Co — External Databases
-- Stores connection info for external Supabase projects.
-- Run this in Supabase > SQL Editor
-- ============================================================

CREATE TABLE IF NOT EXISTS external_databases (
  id           uuid        DEFAULT gen_random_uuid() PRIMARY KEY,
  label        text        NOT NULL,               -- e.g. "Trivia DB"
  supabase_url text        NOT NULL,               -- https://xyz.supabase.co
  supabase_key text        NOT NULL,               -- anon or service key
  default_table text       DEFAULT '',             -- e.g. "trivia_questions"
  created_at   timestamptz DEFAULT now()
);

-- RLS: authenticated users only (admin tool)
ALTER TABLE external_databases ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Auth users can manage external_databases" ON external_databases;
CREATE POLICY "Auth users can manage external_databases"
  ON external_databases FOR ALL TO authenticated USING (true);

-- Helper RPC: get tables from an external Supabase project
-- Called by server.py to validate/list tables before saving.
-- ============================================================
-- DONE. Verify:
--   SELECT * FROM external_databases;
-- ============================================================
