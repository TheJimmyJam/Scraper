import { createClient } from '@supabase/supabase-js'

const supabaseUrl  = import.meta.env.VITE_SUPABASE_URL
const supabaseKey  = import.meta.env.VITE_SUPABASE_ANON_KEY

export const supabase = createClient(supabaseUrl, supabaseKey)

// VITE_API_URL env var takes priority (set in Netlify dashboard)
// Falls back to the Railway cloud backend
export const LOCAL_API = import.meta.env.VITE_API_URL
  || import.meta.env.VITE_LOCAL_API_URL
  || 'https://api-production-ed32.up.railway.app'
