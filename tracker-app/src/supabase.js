import { createClient } from '@supabase/supabase-js'

const supabaseUrl  = import.meta.env.VITE_SUPABASE_URL
const supabaseKey  = import.meta.env.VITE_SUPABASE_ANON_KEY

export const supabase = createClient(supabaseUrl, supabaseKey)

// API URL resolution — works in all three environments:
//
//  1. Netlify (or any external host): set VITE_API_URL in your host's env vars
//     to the Railway service URL.  That value wins above everything else.
//
//  2. Railway (API + React app on same domain): window.location.origin ==
//     the Railway URL, so no env vars are needed at all.
//
//  3. Local dev (npm run dev on localhost): falls back to VITE_LOCAL_API_URL
//     from .env, which should be http://localhost:8000 (your local server.py).
//
const _isLocalhost =
  typeof window !== 'undefined' &&
  (window.location.hostname === 'localhost' ||
   window.location.hostname === '127.0.0.1')

export const API_URL =
  import.meta.env.VITE_API_URL ||
  (_isLocalhost
    ? (import.meta.env.VITE_LOCAL_API_URL || 'http://localhost:8000')
    : window.location.origin)

// Backwards-compat alias — existing code references LOCAL_API
export const LOCAL_API = API_URL
