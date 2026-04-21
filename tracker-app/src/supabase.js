import { createClient } from '@supabase/supabase-js'

const supabaseUrl  = import.meta.env.VITE_SUPABASE_URL
const supabaseKey  = import.meta.env.VITE_SUPABASE_ANON_KEY

export const supabase = createClient(supabaseUrl, supabaseKey)
// VITE_API_URL = your ngrok static domain, e.g. https://rebuild-digital.ngrok-free.app
// Set this in Netlify → Site settings → Environment variables, then redeploy.
export const LOCAL_API = import.meta.env.VITE_API_URL || import.meta.env.VITE_LOCAL_API_URL || 'http://localhost:8000'
