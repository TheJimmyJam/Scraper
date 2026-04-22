import { useState, useEffect, useCallback, useRef } from 'react'
import { supabase, LOCAL_API } from './supabase'

// ── Helpers ──────────────────────────────────────────────────
const STATUS_META = {
  new:              { label: 'New',             color: '#3498db', bg: '#1a3a5c' },
  emailed:          { label: 'Emailed',         color: '#9b59b6', bg: '#2d1a45' },
  follow_up_due:    { label: 'Follow-Up Due',   color: '#f39c12', bg: '#4a3000' },
  follow_up_sent:   { label: 'Follow-Up Sent',  color: '#e67e22', bg: '#3d2000' },
  replied:          { label: 'Replied ★',       color: '#2ecc71', bg: '#0a3020' },
  converted:        { label: 'Converted 🎉',    color: '#c9a96e', bg: '#3a2800' },
  not_interested:   { label: 'Not Interested',  color: '#636380', bg: '#1e1e30' },
}

const SCORE_COLOR = (s) => s >= 7 ? '#e74c3c' : s >= 5 ? '#f39c12' : s >= 3 ? '#3498db' : '#636380'

function Badge({ status }) {
  const m = STATUS_META[status] || STATUS_META.new
  return (
    <span style={{ background: m.bg, color: m.color, padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 700, whiteSpace: 'nowrap' }}>
      {m.label}
    </span>
  )
}

function ScoreBadge({ score }) {
  return (
    <span style={{ background: '#1e1e30', color: SCORE_COLOR(score), border: `1px solid ${SCORE_COLOR(score)}`, padding: '2px 9px', borderRadius: 10, fontSize: 12, fontWeight: 800 }}>
      {score}/10
    </span>
  )
}

function Card({ children, style = {} }) {
  return <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 12, padding: 20, ...style }}>{children}</div>
}

function Btn({ children, onClick, variant = 'primary', disabled = false, style = {}, size = 'md' }) {
  const base = { border: 'none', borderRadius: 8, fontWeight: 600, cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? 0.5 : 1, transition: 'opacity 0.15s', display: 'inline-flex', alignItems: 'center', gap: 6 }
  const sizes = { sm: { padding: '5px 12px', fontSize: 12 }, md: { padding: '8px 16px', fontSize: 14 }, lg: { padding: '11px 22px', fontSize: 15 } }
  const variants = {
    primary:  { background: 'var(--accent)', color: '#fff' },
    secondary:{ background: 'var(--surface2)', color: 'var(--text)' },
    ghost:    { background: 'transparent', color: 'var(--muted)', border: '1px solid var(--border)' },
    danger:   { background: '#3d0a0a', color: '#e74c3c', border: '1px solid #5a1010' },
    success:  { background: '#0a3020', color: '#2ecc71', border: '1px solid #0f5035' },
  }
  return <button onClick={onClick} disabled={disabled} style={{ ...base, ...sizes[size], ...variants[variant], ...style }}>{children}</button>
}

function Input({ label, value, onChange, placeholder, type = 'text', style = {} }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {label && <label style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>{label}</label>}
      <input type={type} value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
        style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '9px 12px', color: 'var(--text)', fontSize: 14, outline: 'none', ...style }} />
    </div>
  )
}

function Select({ label, value, onChange, options, style = {} }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {label && <label style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>{label}</label>}
      <select value={value} onChange={e => onChange(e.target.value)}
        style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '9px 12px', color: 'var(--text)', fontSize: 14, outline: 'none', ...style }}>
        {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </div>
  )
}

function Stat({ label, value, sub, color }) {
  return (
    <Card>
      <div style={{ fontSize: 13, color: 'var(--muted)', marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 32, fontWeight: 800, color: color || 'var(--text)', lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 6 }}>{sub}</div>}
    </Card>
  )
}

function timeAgo(date) {
  if (!date) return '—'
  const d = new Date(date), now = new Date()
  const diff = Math.floor((now - d) / 1000)
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff/60)}m ago`
  if (diff < 86400) return `${Math.floor(diff/3600)}h ago`
  return `${Math.floor(diff/86400)}d ago`
}

function daysUntil(date) {
  if (!date) return null
  const diff = Math.floor((new Date(date) - new Date()) / 86400000)
  return diff
}

function useIsMobile(breakpoint = 768) {
  const [isMobile, setIsMobile] = useState(() => window.innerWidth < breakpoint)
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < breakpoint)
    window.addEventListener('resize', handler)
    return () => window.removeEventListener('resize', handler)
  }, [breakpoint])
  return isMobile
}

// ── Set Password Screen ───────────────────────────────────────
function SetPasswordScreen({ onDone }) {
  const [password, setPassword] = useState('')
  const [confirm,  setConfirm]  = useState('')
  const [error,    setError]    = useState('')
  const [loading,  setLoading]  = useState(false)
  const [done,     setDone]     = useState(false)

  const handleSet = async () => {
    if (password.length < 6) { setError('Password must be at least 6 characters'); return }
    if (password !== confirm)  { setError('Passwords do not match'); return }
    setLoading(true); setError('')
    const { error } = await supabase.auth.updateUser({ password })
    if (error) { setError(error.message); setLoading(false); return }
    setDone(true)
    setTimeout(onDone, 1800)
  }

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
      <div style={{ width: '100%', maxWidth: 400 }}>
        <div style={{ textAlign: 'center', marginBottom: 36 }}>
          <div style={{ fontSize: 36, marginBottom: 10 }}>🔐</div>
          <div style={{ fontSize: 22, fontWeight: 800, color: 'var(--text)' }}>Set Your Password</div>
          <div style={{ fontSize: 13, color: 'var(--muted)', marginTop: 4 }}>Choose a password for your account</div>
        </div>
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 14, padding: 32 }}>
          {done ? (
            <div style={{ textAlign: 'center', padding: '16px 0' }}>
              <div style={{ fontSize: 32, marginBottom: 10 }}>✅</div>
              <div style={{ fontSize: 15, color: '#2ecc71', fontWeight: 600 }}>Password set! Taking you in...</div>
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              <Input label="New Password" value={password} onChange={setPassword} placeholder="At least 6 characters" type="password" />
              <Input label="Confirm Password" value={confirm} onChange={setConfirm} placeholder="Repeat password" type="password" />
              {error && (
                <div style={{ background: '#1a0505', border: '1px solid #5a1010', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#e74c3c' }}>{error}</div>
              )}
              <Btn onClick={handleSet} disabled={loading || !password || !confirm} style={{ width: '100%', justifyContent: 'center', padding: '11px' }}>
                {loading ? '⏳ Setting password...' : 'Set Password & Sign In'}
              </Btn>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Change Password Modal ─────────────────────────────────────
function ChangePasswordModal({ onClose }) {
  const [current,  setCurrent]  = useState('')
  const [password, setPassword] = useState('')
  const [confirm,  setConfirm]  = useState('')
  const [error,    setError]    = useState('')
  const [loading,  setLoading]  = useState(false)
  const [done,     setDone]     = useState(false)

  const handleChange = async () => {
    if (password.length < 6) { setError('Password must be at least 6 characters'); return }
    if (password !== confirm)  { setError('Passwords do not match'); return }
    setLoading(true); setError('')
    const { error } = await supabase.auth.updateUser({ password })
    if (error) { setError(error.message); setLoading(false); return }
    setDone(true)
    setTimeout(onClose, 1800)
  }

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 2000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
         onClick={onClose}>
      <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 14, padding: 32, width: '100%', maxWidth: 380 }}
           onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
          <div style={{ fontSize: 16, fontWeight: 700 }}>Change Password</div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'var(--muted)', fontSize: 20, cursor: 'pointer' }}>✕</button>
        </div>
        {done ? (
          <div style={{ textAlign: 'center', padding: '16px 0' }}>
            <div style={{ fontSize: 28, marginBottom: 8 }}>✅</div>
            <div style={{ color: '#2ecc71', fontWeight: 600 }}>Password updated!</div>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <Input label="New Password" value={password} onChange={setPassword} placeholder="At least 6 characters" type="password" />
            <Input label="Confirm Password" value={confirm} onChange={setConfirm} placeholder="Repeat password" type="password" />
            {error && (
              <div style={{ background: '#1a0505', border: '1px solid #5a1010', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#e74c3c' }}>{error}</div>
            )}
            <Btn onClick={handleChange} disabled={loading || !password || !confirm} style={{ width: '100%', justifyContent: 'center', padding: '11px' }}>
              {loading ? '⏳ Updating...' : 'Update Password'}
            </Btn>
          </div>
        )}
      </div>
    </div>
  )
}

// ── Login Screen ─────────────────────────────────────────────
function LoginScreen() {
  const [email,    setEmail]    = useState('')
  const [password, setPassword] = useState('')
  const [error,    setError]    = useState('')
  const [loading,  setLoading]  = useState(false)
  const [mode,     setMode]     = useState('signin') // 'signin' | 'reset'
  const [resetSent, setResetSent] = useState(false)

  const handleSignIn = async () => {
    setLoading(true); setError('')
    const { error } = await supabase.auth.signInWithPassword({ email, password })
    if (error) setError(error.message)
    setLoading(false)
  }

  const handleReset = async () => {
    setLoading(true); setError('')
    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: window.location.origin,
    })
    if (error) setError(error.message)
    else setResetSent(true)
    setLoading(false)
  }

  const handleKey = (e) => { if (e.key === 'Enter') mode === 'signin' ? handleSignIn() : handleReset() }

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
      <div style={{ width: '100%', maxWidth: 400 }}>
        {/* Logo */}
        <div style={{ textAlign: 'center', marginBottom: 36 }}>
          <div style={{ fontSize: 36, marginBottom: 10 }}>🔨</div>
          <div style={{ fontSize: 22, fontWeight: 800, color: 'var(--text)', letterSpacing: -0.5 }}>Rebuild Digital Co</div>
          <div style={{ fontSize: 13, color: 'var(--muted)', marginTop: 4 }}>Outreach Tracker</div>
        </div>

        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 14, padding: 32 }}>
          <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 22, color: 'var(--text)' }}>
            {mode === 'signin' ? 'Sign In' : 'Reset Password'}
          </div>

          {resetSent ? (
            <div style={{ textAlign: 'center', padding: '16px 0' }}>
              <div style={{ fontSize: 28, marginBottom: 10 }}>📧</div>
              <div style={{ fontSize: 14, color: '#2ecc71' }}>Check your email for a reset link.</div>
              <button onClick={() => { setMode('signin'); setResetSent(false) }}
                style={{ marginTop: 16, background: 'none', border: 'none', color: 'var(--accent)', fontSize: 13, cursor: 'pointer', fontWeight: 600 }}>
                Back to sign in
              </button>
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
              <Input label="Email" value={email} onChange={setEmail} placeholder="you@example.com" type="email" />
              {mode === 'signin' && (
                <Input label="Password" value={password} onChange={setPassword} placeholder="••••••••" type="password" />
              )}
              {error && (
                <div style={{ background: '#1a0505', border: '1px solid #5a1010', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#e74c3c' }}>
                  {error}
                </div>
              )}
              <Btn onClick={mode === 'signin' ? handleSignIn : handleReset} disabled={loading || !email || (mode === 'signin' && !password)} style={{ width: '100%', justifyContent: 'center', padding: '11px' }}>
                {loading ? '⏳ Please wait...' : mode === 'signin' ? 'Sign In' : 'Send Reset Link'}
              </Btn>
              <button onClick={() => { setMode(mode === 'signin' ? 'reset' : 'signin'); setError('') }}
                style={{ background: 'none', border: 'none', color: 'var(--muted)', fontSize: 12, cursor: 'pointer', textAlign: 'center', marginTop: 4 }}>
                {mode === 'signin' ? 'Forgot password?' : '← Back to sign in'}
              </button>
            </div>
          )}
        </div>

        <div style={{ textAlign: 'center', marginTop: 20, fontSize: 12, color: 'var(--muted)' }}>
          Access is by invitation only. Contact your admin to get an account.
        </div>
      </div>
    </div>
  )
}

// ── Dashboard ────────────────────────────────────────────────
function Dashboard({ businesses, emailLogs, followUps, feedback, scrapeRuns }) {
  const isMobile   = useIsMobile()
  const total      = businesses.length
  const emailed    = businesses.filter(b => ['emailed','follow_up_due','follow_up_sent','replied','converted'].includes(b.status)).length
  const replies    = businesses.filter(b => ['replied','converted'].includes(b.status)).length
  const converted  = businesses.filter(b => b.status === 'converted').length
  const dueFollowUps = followUps.filter(f => f.status === 'pending' && daysUntil(f.scheduled_for) <= 0).length
  const highOpp    = businesses.filter(b => b.opportunity_score >= 7).length
  const replyRate  = emailed > 0 ? Math.round((replies / emailed) * 100) : 0

  const recentActivity = [
    ...emailLogs.slice(0, 5).map(e => ({ type: 'email', text: `Sent email to ${businesses.find(b=>b.id===e.business_id)?.name || '?'}`, time: e.sent_at })),
    ...feedback.slice(0, 5).map(f => ({ type: 'reply', text: `Reply from ${businesses.find(b=>b.id===f.business_id)?.name || '?'}`, time: f.received_at })),
  ].sort((a, b) => new Date(b.time) - new Date(a.time)).slice(0, 8)

  const lastRun = scrapeRuns[0]

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 20 }}>Dashboard</h2>

      {/* Stats row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 14, marginBottom: 24 }}>
        <Stat label="Total Businesses" value={total} sub={`${highOpp} high opportunity`} />
        <Stat label="Emails Sent" value={emailed} sub={`of ${total} scraped`} color="#9b59b6" />
        <Stat label="Replies" value={replies} sub={`${replyRate}% reply rate`} color="#2ecc71" />
        <Stat label="Converted" value={converted} sub="paying clients" color="#c9a96e" />
        <Stat label="Follow-Ups Due" value={dueFollowUps} sub="need attention" color={dueFollowUps > 0 ? '#f39c12' : 'var(--text)'} />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr', gap: 16 }}>
        {/* Recent activity */}
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Recent Activity</div>
          {recentActivity.length === 0
            ? <div style={{ color: 'var(--muted)', fontSize: 14 }}>No activity yet. Run the scraper to get started.</div>
            : recentActivity.map((a, i) => (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '9px 0', borderBottom: i < recentActivity.length-1 ? '1px solid var(--border)' : 'none' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span style={{ fontSize: 16 }}>{a.type === 'email' ? '📧' : '💬'}</span>
                  <span style={{ fontSize: 14 }}>{a.text}</span>
                </div>
                <span style={{ fontSize: 12, color: 'var(--muted)' }}>{timeAgo(a.time)}</span>
              </div>
            ))}
        </Card>

        {/* Status breakdown */}
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Pipeline Breakdown</div>
          {Object.entries(STATUS_META).map(([key, meta]) => {
            const count = businesses.filter(b => b.status === key).length
            const pct = total > 0 ? (count / total) * 100 : 0
            return (
              <div key={key} style={{ marginBottom: 12 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                  <span style={{ fontSize: 13, color: meta.color }}>{meta.label}</span>
                  <span style={{ fontSize: 13, color: 'var(--muted)' }}>{count}</span>
                </div>
                <div style={{ background: 'var(--border)', borderRadius: 4, height: 5 }}>
                  <div style={{ background: meta.color, width: `${pct}%`, height: '100%', borderRadius: 4, transition: 'width 0.5s' }} />
                </div>
              </div>
            )
          })}
        </Card>
      </div>

      {/* Last scrape run */}
      {lastRun && (
        <Card style={{ marginTop: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Last Scrape Run</div>
          <div style={{ display: 'flex', gap: 32, flexWrap: 'wrap' }}>
            <div><div style={{ fontSize: 12, color: 'var(--muted)' }}>Location</div><div style={{ fontSize: 14 }}>{lastRun.location}</div></div>
            <div><div style={{ fontSize: 12, color: 'var(--muted)' }}>Started</div><div style={{ fontSize: 14 }}>{new Date(lastRun.started_at).toLocaleString()}</div></div>
            <div><div style={{ fontSize: 12, color: 'var(--muted)' }}>Status</div><div style={{ fontSize: 14, color: lastRun.status === 'completed' ? '#2ecc71' : lastRun.status === 'failed' ? '#e74c3c' : '#f39c12' }}>{lastRun.status}</div></div>
            <div><div style={{ fontSize: 12, color: 'var(--muted)' }}>Found</div><div style={{ fontSize: 14 }}>{lastRun.businesses_found} businesses</div></div>
            <div><div style={{ fontSize: 12, color: 'var(--muted)' }}>Emails Queued</div><div style={{ fontSize: 14 }}>{lastRun.emails_queued}</div></div>
          </div>
        </Card>
      )}
    </div>
  )
}

// ── Businesses Table ─────────────────────────────────────────
function BusinessesView({ businesses, onStatusChange, onSelect, runFilter, onClearRunFilter }) {
  const isMobile = useIsMobile()
  const [search, setSearch]     = useState('')
  const [filterStatus, setFilterStatus] = useState('all')
  const [filterCat, setFilterCat]       = useState('all')
  const [sortBy, setSortBy]     = useState('opportunity_score')
  const [sortDir, setSortDir]   = useState('desc')

  const categories = ['all', ...new Set(businesses.map(b => b.category).filter(Boolean))]

  const filtered = businesses
    .filter(b => {
      if (runFilter && b.scrape_run_id !== runFilter.id) return false
      if (filterStatus !== 'all' && b.status !== filterStatus) return false
      if (filterCat !== 'all' && b.category !== filterCat) return false
      if (search) {
        const q = search.toLowerCase()
        return (b.name || '').toLowerCase().includes(q) ||
               (b.website || '').toLowerCase().includes(q) ||
               (b.email || '').toLowerCase().includes(q)
      }
      return true
    })
    .sort((a, b) => {
      const av = a[sortBy] ?? 0, bv = b[sortBy] ?? 0
      return sortDir === 'desc' ? (bv > av ? 1 : -1) : (av > bv ? 1 : -1)
    })

  const toggleSort = (col) => {
    if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
  }

  const Th = ({ col, children }) => (
    <th onClick={() => toggleSort(col)} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, cursor: 'pointer', whiteSpace: 'nowrap', userSelect: 'none' }}>
      {children} {sortBy === col ? (sortDir === 'desc' ? '↓' : '↑') : ''}
    </th>
  )

  return (
    <div>
      {/* Run filter banner */}
      {runFilter && (
        <div style={{ background: '#1a2a1a', border: '1px solid #0f5035', borderRadius: 10, padding: '10px 16px', marginBottom: 14, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 13, color: '#2ecc71' }}>
            🔍 Showing <strong>{filtered.length} businesses</strong> from scrape on {new Date(runFilter.started_at).toLocaleString()} · {runFilter.categories || 'all categories'}
          </span>
          <button onClick={onClearRunFilter} style={{ background: 'none', border: '1px solid #0f5035', borderRadius: 6, color: '#2ecc71', fontSize: 12, padding: '4px 10px', cursor: 'pointer', fontWeight: 600 }}>✕ Clear Filter</button>
        </div>
      )}
      <div style={{ marginBottom: 18 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 10, marginBottom: 10 }}>
          <h2 style={{ fontSize: 22, fontWeight: 800 }}>Businesses <span style={{ fontSize: 15, color: 'var(--muted)', fontWeight: 400 }}>({filtered.length})</span></h2>
        </div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search name, email, site..." style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '7px 12px', color: 'var(--text)', fontSize: 14, outline: 'none', flex: isMobile ? '1 1 100%' : '0 0 220px' }} />
          <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)} style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '7px 12px', color: 'var(--text)', fontSize: 14, outline: 'none', flex: isMobile ? '1 1 calc(50% - 4px)' : '0 0 auto' }}>
            <option value="all">All Statuses</option>
            {Object.entries(STATUS_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
          </select>
          <select value={filterCat} onChange={e => setFilterCat(e.target.value)} style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '7px 12px', color: 'var(--text)', fontSize: 14, outline: 'none', flex: isMobile ? '1 1 calc(50% - 4px)' : '0 0 auto' }}>
            {categories.map(c => <option key={c} value={c}>{c === 'all' ? 'All Categories' : c}</option>)}
          </select>
        </div>
      </div>

      {isMobile ? (
        /* ── Mobile: card list ── */
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {filtered.length === 0
            ? <div style={{ padding: 32, textAlign: 'center', color: 'var(--muted)' }}>No businesses match your filters.</div>
            : filtered.map(biz => (
            <div key={biz.id} onClick={() => onSelect(biz)}
              style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 12, padding: '14px 16px', cursor: 'pointer', display: 'flex', flexDirection: 'column', gap: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 700, fontSize: 15, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{biz.name}</div>
                  <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>{biz.category}</div>
                </div>
                <ScoreBadge score={biz.opportunity_score || 0} />
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <select value={biz.status} onChange={e => { e.stopPropagation(); onStatusChange(biz.id, e.target.value) }}
                  onClick={e => e.stopPropagation()}
                  style={{ background: STATUS_META[biz.status]?.bg || 'var(--surface2)', color: STATUS_META[biz.status]?.color || 'var(--text)', border: 'none', borderRadius: 8, padding: '5px 8px', fontSize: 12, fontWeight: 700, cursor: 'pointer', outline: 'none' }}>
                  {Object.entries(STATUS_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
                </select>
                <Btn size="sm" variant="ghost" onClick={e => { e.stopPropagation(); onSelect(biz) }}>View →</Btn>
              </div>
              {biz.email && <div style={{ fontSize: 12, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>📧 {biz.email}</div>}
            </div>
          ))}
        </div>
      ) : (
        /* ── Desktop: table ── */
        <div style={{ overflowX: 'auto', borderRadius: 12, border: '1px solid var(--border)' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', background: 'var(--surface)' }}>
            <thead style={{ background: 'var(--surface2)' }}>
              <tr>
                <Th col="name">Business</Th>
                <Th col="category">Category</Th>
                <Th col="opportunity_score">Score</Th>
                <Th col="status">Status</Th>
                <Th col="email">Email</Th>
                <th style={{ padding: '10px 14px', fontSize: 12, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1 }}>Issues</th>
                <th style={{ padding: '10px 14px' }}></th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0
                ? <tr><td colSpan={7} style={{ padding: 32, textAlign: 'center', color: 'var(--muted)' }}>No businesses match your filters.</td></tr>
                : filtered.map((biz, i) => (
                <tr key={biz.id} style={{ borderTop: '1px solid var(--border)', cursor: 'pointer' }}
                    onMouseEnter={e => e.currentTarget.style.background = 'var(--surface2)'}
                    onMouseLeave={e => e.currentTarget.style.background = ''}
                    onClick={() => onSelect(biz)}>
                  <td style={{ padding: '12px 14px' }}>
                    <div style={{ fontWeight: 600, fontSize: 14 }}>{biz.name}</div>
                    {biz.website && <a href={biz.website} target="_blank" rel="noreferrer" onClick={e => e.stopPropagation()} style={{ fontSize: 12, color: 'var(--accent)', textDecoration: 'none' }}>{biz.website.replace(/https?:\/\//, '')}</a>}
                  </td>
                  <td style={{ padding: '12px 14px', fontSize: 13, color: 'var(--muted)' }}>{biz.category}</td>
                  <td style={{ padding: '12px 14px' }}><ScoreBadge score={biz.opportunity_score || 0} /></td>
                  <td style={{ padding: '12px 14px' }}>
                    <select value={biz.status} onChange={e => { e.stopPropagation(); onStatusChange(biz.id, e.target.value) }}
                      onClick={e => e.stopPropagation()}
                      style={{ background: STATUS_META[biz.status]?.bg || 'var(--surface2)', color: STATUS_META[biz.status]?.color || 'var(--text)', border: 'none', borderRadius: 8, padding: '4px 8px', fontSize: 12, fontWeight: 700, cursor: 'pointer', outline: 'none' }}>
                      {Object.entries(STATUS_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
                    </select>
                  </td>
                  <td style={{ padding: '12px 14px', fontSize: 13 }}>{biz.email || <span style={{ color: 'var(--muted)' }}>—</span>}</td>
                  <td style={{ padding: '12px 14px', fontSize: 12, color: 'var(--muted)', maxWidth: 220 }}>
                    <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{biz.issues || '—'}</div>
                  </td>
                  <td style={{ padding: '12px 14px' }}><Btn size="sm" variant="ghost" onClick={e => { e.stopPropagation(); onSelect(biz) }}>View →</Btn></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

// ── Business Detail Modal ────────────────────────────────────
function BusinessModal({ biz, emailLogs, followUps, feedback, onClose, onStatusChange, onAddNote, onAddFeedback, onScheduleFollowUp, apiStatus, onRefresh, showToast }) {
  const isMobile = useIsMobile()
  const [note, setNote]             = useState(biz.notes || '')
  const [fbMsg, setFbMsg]           = useState('')
  const [fbSentiment, setFbSentiment] = useState('neutral')
  const [fuDays, setFuDays]         = useState('5')
  const [sending, setSending]       = useState(false)
  const [localEmail, setLocalEmail] = useState(biz.email || '')
  const [editingEmail, setEditingEmail] = useState(false)
  const [emailDraft, setEmailDraft] = useState(biz.email || '')
  const [savingEmail, setSavingEmail] = useState(false)

  const handleSaveEmail = async () => {
    setSavingEmail(true)
    const trimmed = emailDraft.trim()
    await supabase.from('businesses').update({ email: trimmed }).eq('id', biz.id)
    setLocalEmail(trimmed)
    setEditingEmail(false)
    setSavingEmail(false)
    showToast('Email saved')
    onRefresh()
  }

  const handleViewProposal = () => {
    window.open(`${LOCAL_API}/preview-proposal?business_id=${biz.id}`, '_blank')
  }

  const handleSendProposal = async () => {
    if (!localEmail) { showToast('No email address on file', 'error'); return }
    if (!window.confirm(`Send proposal to ${localEmail}?`)) return
    setSending(true)
    try {
      const res = await fetch(`${LOCAL_API}/send-proposal`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ business_id: biz.id }),
      })
      const data = await res.json()
      if (data.error) {
        showToast(data.error, 'error')
      } else {
        showToast(`Proposal sent to ${localEmail}!`, 'success')
        onRefresh()
      }
    } catch {
      showToast('Railway API offline — check Settings tab', 'error')
    }
    setSending(false)
  }

  const bizEmails   = emailLogs.filter(e => e.business_id === biz.id)
  const bizFollowUps = followUps.filter(f => f.business_id === biz.id)
  const bizFeedback = feedback.filter(f => f.business_id === biz.id)

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 1000, display: 'flex', alignItems: isMobile ? 'flex-end' : 'flex-start', justifyContent: isMobile ? 'stretch' : 'flex-end' }}
         onClick={onClose}>
      <div style={isMobile
        ? { width: '100%', maxHeight: '92vh', background: 'var(--surface)', overflowY: 'auto', padding: '20px 16px 32px', borderRadius: '18px 18px 0 0' }
        : { width: 520, height: '100vh', background: 'var(--surface)', overflowY: 'auto', padding: 28 }}
           onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
          <div>
            <h3 style={{ fontSize: 20, fontWeight: 800 }}>{biz.name}</h3>
            <div style={{ fontSize: 13, color: 'var(--muted)', marginTop: 3 }}>{biz.category} · {biz.address}</div>
          </div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'var(--muted)', fontSize: 20, cursor: 'pointer' }}>✕</button>
        </div>

        {/* Status + score */}
        <div style={{ display: 'flex', gap: 10, marginBottom: 20, alignItems: 'center' }}>
          <Badge status={biz.status} />
          <ScoreBadge score={biz.opportunity_score || 0} />
          <select value={biz.status} onChange={e => onStatusChange(biz.id, e.target.value)}
            style={{ marginLeft: 'auto', background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '5px 10px', color: 'var(--text)', fontSize: 13, cursor: 'pointer', outline: 'none' }}>
            {Object.entries(STATUS_META).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
          </select>
        </div>

        {/* Contact info */}
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Contact Info</div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, fontSize: 13 }}>
            <div style={{ gridColumn: '1/-1' }}>
              <div style={{ color: 'var(--muted)', fontSize: 11, marginBottom: 4 }}>Email</div>
              {editingEmail ? (
                <div style={{ display: 'flex', gap: 7, alignItems: 'center' }}>
                  <input
                    value={emailDraft}
                    onChange={e => setEmailDraft(e.target.value)}
                    onKeyDown={e => e.key === 'Enter' && handleSaveEmail()}
                    placeholder="email@business.com"
                    autoFocus
                    style={{ flex: 1, background: 'var(--surface2)', border: '1px solid var(--accent)', borderRadius: 7, padding: '6px 10px', color: 'var(--text)', fontSize: 13, outline: 'none' }}
                  />
                  <Btn size="sm" onClick={handleSaveEmail} disabled={savingEmail}>{savingEmail ? '...' : 'Save'}</Btn>
                  <Btn size="sm" variant="ghost" onClick={() => { setEditingEmail(false); setEmailDraft(localEmail) }}>Cancel</Btn>
                </div>
              ) : (
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <span style={{ color: localEmail ? 'var(--text)' : 'var(--muted)' }}>{localEmail || '—'}</span>
                  <button onClick={() => { setEmailDraft(localEmail); setEditingEmail(true) }}
                    style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 5, color: 'var(--muted)', fontSize: 11, padding: '2px 8px', cursor: 'pointer', fontWeight: 600 }}>
                    ✏ Edit
                  </button>
                </div>
              )}
            </div>
            <div><div style={{ color: 'var(--muted)', fontSize: 11, marginBottom: 2 }}>Phone</div><div>{biz.phone || '—'}</div></div>
            <div style={{ gridColumn: '1/-1' }}><div style={{ color: 'var(--muted)', fontSize: 11, marginBottom: 2 }}>Website</div>
              {biz.website ? <a href={biz.website} target="_blank" rel="noreferrer" style={{ color: 'var(--accent)', fontSize: 13 }}>{biz.website}</a> : '—'}</div>
          </div>
        </Card>

        {/* Proposal actions */}
        <Card style={{ marginBottom: 16, background: '#0a1a0a', borderColor: '#0f5035' }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 12 }}>Proposal Email</div>
          <div style={{ display: 'flex', gap: 10 }}>
            <Btn variant="ghost" onClick={handleViewProposal} disabled={apiStatus !== 'online'} style={{ flex: 1, justifyContent: 'center' }}>
              👁 View Proposal
            </Btn>
            <Btn variant="success" onClick={handleSendProposal} disabled={sending || apiStatus !== 'online' || !localEmail} style={{ flex: 1, justifyContent: 'center' }}>
              {sending ? '⏳ Sending...' : '📤 Send Proposal'}
            </Btn>
          </div>
          {!localEmail && (
            <div style={{ fontSize: 12, color: '#e74c3c', marginTop: 8 }}>⚠ No email on file — click ✏ Edit above to add one.</div>
          )}
          {apiStatus !== 'online' && (
            <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 8 }}>Railway API offline — check Settings tab.</div>
          )}
        </Card>

        {/* Issues */}
        {biz.issues && (
          <Card style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Issues Found</div>
            {biz.issues.split('|').map((issue, i) => (
              <div key={i} style={{ fontSize: 13, color: '#f39c12', padding: '4px 0', borderBottom: i < biz.issues.split('|').length - 1 ? '1px solid var(--border)' : 'none' }}>
                ⚠ {issue.trim()}
              </div>
            ))}
          </Card>
        )}

        {/* Email history */}
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Email History ({bizEmails.length})</div>
          {bizEmails.length === 0
            ? <div style={{ fontSize: 13, color: 'var(--muted)' }}>No emails sent yet.</div>
            : bizEmails.map(e => (
            <div key={e.id} style={{ padding: '8px 0', borderBottom: '1px solid var(--border)', fontSize: 13 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 600 }}>{e.email_type === 'initial' ? '📧 Initial' : `🔁 Follow-Up #${e.email_type.split('_').pop()}`}</span>
                <span style={{ color: 'var(--muted)', fontSize: 12 }}>{new Date(e.sent_at).toLocaleDateString()}</span>
              </div>
              <div style={{ color: 'var(--muted)', marginTop: 2 }}>{e.subject}</div>
              <div style={{ marginTop: 3 }}>
                <span style={{ fontSize: 11, color: e.status === 'replied' ? '#2ecc71' : e.status === 'failed' ? '#e74c3c' : '#9b59b6', fontWeight: 700 }}>{e.status.toUpperCase()}</span>
              </div>
            </div>
          ))}
        </Card>

        {/* Follow-ups */}
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Follow-Ups</div>
          {bizFollowUps.length === 0
            ? <div style={{ fontSize: 13, color: 'var(--muted)', marginBottom: 10 }}>None scheduled.</div>
            : bizFollowUps.map(f => {
            const days = daysUntil(f.scheduled_for)
            const overdue = days !== null && days < 0
            return (
              <div key={f.id} style={{ padding: '7px 0', borderBottom: '1px solid var(--border)', fontSize: 13, display: 'flex', justifyContent: 'space-between' }}>
                <span>Follow-Up #{f.follow_up_number} · <span style={{ color: f.status === 'sent' ? '#2ecc71' : overdue ? '#e74c3c' : '#f39c12' }}>{f.status === 'sent' ? 'Sent' : overdue ? `Overdue ${Math.abs(days)}d` : `In ${days}d`}</span></span>
                <span style={{ color: 'var(--muted)', fontSize: 12 }}>{new Date(f.scheduled_for).toLocaleDateString()}</span>
              </div>
            )
          })}
          <div style={{ display: 'flex', gap: 8, marginTop: 10, alignItems: 'center' }}>
            <input type="number" value={fuDays} onChange={e => setFuDays(e.target.value)} min="1" max="60"
              style={{ width: 60, background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 6, padding: '6px 8px', color: 'var(--text)', fontSize: 13, outline: 'none' }} />
            <span style={{ fontSize: 13, color: 'var(--muted)' }}>days from now</span>
            <Btn size="sm" onClick={() => onScheduleFollowUp(biz.id, parseInt(fuDays), bizFollowUps.length + 1)}>Schedule</Btn>
          </div>
        </Card>

        {/* Feedback */}
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Feedback / Replies ({bizFeedback.length})</div>
          {bizFeedback.map(f => (
            <div key={f.id} style={{ padding: '8px 0', borderBottom: '1px solid var(--border)', fontSize: 13 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                <span style={{ color: f.sentiment === 'positive' ? '#2ecc71' : f.sentiment === 'negative' ? '#e74c3c' : '#f39c12', fontWeight: 700, fontSize: 12 }}>{f.sentiment?.toUpperCase()} · {f.channel}</span>
                <span style={{ color: 'var(--muted)', fontSize: 12 }}>{timeAgo(f.received_at)}</span>
              </div>
              <div>{f.message}</div>
              {f.action_taken && <div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 3 }}>Action: {f.action_taken}</div>}
            </div>
          ))}
          <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <textarea value={fbMsg} onChange={e => setFbMsg(e.target.value)} placeholder="Log a reply or feedback note..."
              style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: 10, color: 'var(--text)', fontSize: 13, resize: 'vertical', minHeight: 70, outline: 'none', width: '100%' }} />
            <div style={{ display: 'flex', gap: 8 }}>
              <select value={fbSentiment} onChange={e => setFbSentiment(e.target.value)}
                style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '6px 10px', color: 'var(--text)', fontSize: 13, outline: 'none' }}>
                <option value="positive">👍 Positive</option>
                <option value="neutral">😐 Neutral</option>
                <option value="negative">👎 Negative</option>
              </select>
              <Btn size="sm" onClick={() => { onAddFeedback(biz.id, fbMsg, fbSentiment); setFbMsg('') }} disabled={!fbMsg.trim()}>Log Feedback</Btn>
            </div>
          </div>
        </Card>

        {/* Notes */}
        <Card>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>Notes</div>
          <textarea value={note} onChange={e => setNote(e.target.value)}
            style={{ background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: 10, color: 'var(--text)', fontSize: 13, resize: 'vertical', minHeight: 80, outline: 'none', width: '100%', marginBottom: 8 }} />
          <Btn size="sm" onClick={() => onAddNote(biz.id, note)}>Save Note</Btn>
        </Card>
      </div>
    </div>
  )
}

// ── Email Tracker ────────────────────────────────────────────
function EmailsView({ emailLogs, businesses }) {
  const bName = (id) => businesses.find(b => b.id === id)?.name || 'Unknown'
  const sent     = emailLogs.length
  const replied  = emailLogs.filter(e => e.status === 'replied').length
  const failed   = emailLogs.filter(e => e.status === 'failed').length

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 18 }}>Email Tracker</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 14, marginBottom: 22 }}>
        <Stat label="Total Sent" value={sent} />
        <Stat label="Replied" value={replied} color="#2ecc71" sub={sent > 0 ? `${Math.round(replied/sent*100)}% rate` : ''} />
        <Stat label="Failed" value={failed} color="#e74c3c" />
        <Stat label="Pending Reply" value={sent - replied - failed} color="#9b59b6" />
      </div>

      <div style={{ borderRadius: 12, border: '1px solid var(--border)', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', background: 'var(--surface)' }}>
          <thead style={{ background: 'var(--surface2)' }}>
            <tr>
              {['Business', 'Type', 'Subject', 'Sent', 'Status'].map(h => (
                <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {emailLogs.length === 0
              ? <tr><td colSpan={5} style={{ padding: 32, textAlign: 'center', color: 'var(--muted)' }}>No emails sent yet.</td></tr>
              : emailLogs.map(e => (
              <tr key={e.id} style={{ borderTop: '1px solid var(--border)' }}>
                <td style={{ padding: '11px 14px', fontWeight: 600, fontSize: 14 }}>{bName(e.business_id)}</td>
                <td style={{ padding: '11px 14px', fontSize: 13 }}>
                  <span style={{ color: e.email_type === 'initial' ? '#9b59b6' : '#e67e22' }}>
                    {e.email_type === 'initial' ? '📧 Initial' : `🔁 Follow-Up`}
                  </span>
                </td>
                <td style={{ padding: '11px 14px', fontSize: 13, color: 'var(--muted)', maxWidth: 240, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.subject}</td>
                <td style={{ padding: '11px 14px', fontSize: 13, color: 'var(--muted)', whiteSpace: 'nowrap' }}>{new Date(e.sent_at).toLocaleDateString()}</td>
                <td style={{ padding: '11px 14px' }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: e.status === 'replied' ? '#2ecc71' : e.status === 'failed' ? '#e74c3c' : e.status === 'sent' ? '#9b59b6' : 'var(--muted)' }}>
                    {e.status.toUpperCase()}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Follow-Up Manager ────────────────────────────────────────
function FollowUpsView({ followUps, businesses, onMarkSent, onSendFollowUp }) {
  const biz = (id) => businesses.find(b => b.id === id)
  const overdue  = followUps.filter(f => f.status === 'pending' && daysUntil(f.scheduled_for) <= 0)
  const upcoming = followUps.filter(f => f.status === 'pending' && daysUntil(f.scheduled_for) > 0)
  const sent     = followUps.filter(f => f.status === 'sent')

  const FollowUpRow = ({ f, urgent }) => {
    const b = biz(f.business_id)
    const days = daysUntil(f.scheduled_for)
    return (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '12px 0', borderBottom: '1px solid var(--border)', flexWrap: 'wrap', gap: 10 }}>
        <div>
          <div style={{ fontWeight: 600, fontSize: 14 }}>{b?.name || 'Unknown'}</div>
          <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>{b?.email || 'No email'} · Follow-Up #{f.follow_up_number}</div>
          {f.notes && <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>{f.notes}</div>}
        </div>
        <div style={{ display: 'flex', align: 'center', gap: 10 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: urgent ? '#e74c3c' : '#f39c12' }}>
            {days < 0 ? `${Math.abs(days)}d overdue` : `in ${days}d`}
          </span>
          {b?.email && <Btn size="sm" variant={urgent ? 'danger' : 'secondary'} onClick={() => onSendFollowUp(f, b)}>Send Now</Btn>}
          <Btn size="sm" variant="ghost" onClick={() => onMarkSent(f.id)}>Mark Sent</Btn>
        </div>
      </div>
    )
  }

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 18 }}>Follow-Up Manager</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 14, marginBottom: 24 }}>
        <Stat label="Overdue" value={overdue.length} color={overdue.length > 0 ? '#e74c3c' : 'var(--text)'} />
        <Stat label="Upcoming" value={upcoming.length} color="#f39c12" />
        <Stat label="Sent" value={sent.length} color="#2ecc71" />
      </div>

      {overdue.length > 0 && (
        <Card style={{ marginBottom: 16, borderColor: '#5a1010' }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: '#e74c3c', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>🔴 Overdue ({overdue.length})</div>
          {overdue.map(f => <FollowUpRow key={f.id} f={f} urgent={true} />)}
        </Card>
      )}

      {upcoming.length > 0 && (
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: '#f39c12', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>🟡 Upcoming ({upcoming.length})</div>
          {upcoming.map(f => <FollowUpRow key={f.id} f={f} urgent={false} />)}
        </Card>
      )}

      {sent.length > 0 && (
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: '#2ecc71', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>✅ Sent ({sent.length})</div>
          {sent.slice(0, 10).map(f => {
            const b = biz(f.business_id)
            return (
              <div key={f.id} style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--border)', fontSize: 13 }}>
                <span>{b?.name || 'Unknown'} — Follow-Up #{f.follow_up_number}</span>
                <span style={{ color: 'var(--muted)' }}>{f.sent_at ? new Date(f.sent_at).toLocaleDateString() : '—'}</span>
              </div>
            )
          })}
        </Card>
      )}

      {overdue.length === 0 && upcoming.length === 0 && sent.length === 0 && (
        <Card><div style={{ color: 'var(--muted)', fontSize: 14 }}>No follow-ups scheduled. Follow-ups are created automatically when you send an initial email.</div></Card>
      )}
    </div>
  )
}

// ── Feedback Log ─────────────────────────────────────────────
function FeedbackView({ feedback, businesses }) {
  const bName = (id) => businesses.find(b => b.id === id)?.name || 'Unknown'
  const pos = feedback.filter(f => f.sentiment === 'positive').length
  const neg = feedback.filter(f => f.sentiment === 'negative').length
  const neu = feedback.filter(f => f.sentiment === 'neutral').length

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 18 }}>Feedback & Replies</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 14, marginBottom: 22 }}>
        <Stat label="Total" value={feedback.length} />
        <Stat label="Positive" value={pos} color="#2ecc71" />
        <Stat label="Neutral" value={neu} color="#f39c12" />
        <Stat label="Negative" value={neg} color="#e74c3c" />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {feedback.length === 0
          ? <Card><div style={{ color: 'var(--muted)' }}>No feedback logged yet. Use the business detail panel to log replies.</div></Card>
          : feedback.map(f => (
          <Card key={f.id}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
              <div>
                <span style={{ fontWeight: 700, fontSize: 15 }}>{bName(f.business_id)}</span>
                <span style={{ marginLeft: 10, fontSize: 12, color: f.sentiment === 'positive' ? '#2ecc71' : f.sentiment === 'negative' ? '#e74c3c' : '#f39c12', fontWeight: 700, textTransform: 'uppercase' }}>
                  {f.sentiment === 'positive' ? '👍' : f.sentiment === 'negative' ? '👎' : '😐'} {f.sentiment}
                </span>
              </div>
              <span style={{ fontSize: 12, color: 'var(--muted)' }}>{timeAgo(f.received_at)} · {f.channel}</span>
            </div>
            <p style={{ fontSize: 14, color: 'var(--text)', lineHeight: 1.6 }}>{f.message}</p>
            {f.action_taken && <div style={{ marginTop: 8, fontSize: 12, color: 'var(--muted)', borderTop: '1px solid var(--border)', paddingTop: 8 }}>Action taken: {f.action_taken}</div>}
          </Card>
        ))}
      </div>
    </div>
  )
}

// ── Admin Panel ──────────────────────────────────────────────
const ALL_CATEGORIES = [
  'Hair Salon', 'Restaurant', 'Contractor', 'Medical Office',
  'Plumber', 'Electrician', 'Dentist', 'Gym / Fitness', 'Auto Repair', 'Landscaping'
]

function AdminView({ scrapeRuns, onRunScraper, apiStatus, onCheckApi, onViewRun, onResetDb, userRole, onScrapeComplete }) {
  const [location,    setLocation]    = useState('Dallas, TX')
  const [limit,       setLimit]       = useState('10')
  const [sendEmails,  setSendEmails]  = useState(false)
  const [running,     setRunning]     = useState(false)
  const [log,         setLog]         = useState([])
  const [selectedCats, setSelectedCats] = useState(new Set())
  const [customCats,   setCustomCats]   = useState([])
  const [customInput,  setCustomInput]  = useState('')
  const [resetStep,   setResetStep]   = useState(0)  // kept for any future admin use
  const logBoxRef = useRef(null)

  const toggleCat = (cat) => setSelectedCats(prev => {
    const next = new Set(prev)
    next.has(cat) ? next.delete(cat) : next.add(cat)
    return next
  })
  const toggleAll = () => setSelectedCats(
    selectedCats.size === ALL_CATEGORIES.length ? new Set() : new Set(ALL_CATEGORIES)
  )

  const addCustomCat = () => {
    const val = customInput.trim()
    if (!val) return
    const normalized = val.charAt(0).toUpperCase() + val.slice(1)
    if (!customCats.includes(normalized) && !ALL_CATEGORIES.includes(normalized)) {
      setCustomCats(prev => [...prev, normalized])
    }
    setCustomInput('')
  }
  const removeCustomCat = (cat) => setCustomCats(prev => prev.filter(c => c !== cat))
  const handleCustomKey = (e) => { if (e.key === 'Enter') addCustomCat() }

  const totalCatCount = selectedCats.size + customCats.length

  const handleResetClick = async () => {
    if (resetStep === 0) { setResetStep(1); return }
    if (resetStep === 1) {
      setResetStep(2)
      await onResetDb()
      setResetStep(0)
    }
  }

  const handleRun = async () => {
    if (totalCatCount === 0) return
    setRunning(true)
    setLog(['⏳ Starting scraper...'])

    try {
      await onRunScraper({
        location,
        limit: parseInt(limit),
        send_emails: sendEmails,
        categories: [...selectedCats, ...customCats],
      })

      // Poll /status every 2s to stream live server logs
      const poll = setInterval(async () => {
        try {
          const res = await fetch(`${LOCAL_API}/status`, { signal: AbortSignal.timeout(3000) })
          const data = await res.json()
          if (data.log?.length) {
            setLog(data.log)
            // Auto-scroll to bottom
            if (logBoxRef.current) logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight
          }
          if (!data.running) {
            clearInterval(poll)
            setRunning(false)
            // Silently refresh businesses + scrape history without unmounting this view
            if (onScrapeComplete) onScrapeComplete()
          }
        } catch {
          clearInterval(poll)
          setRunning(false)
        }
      }, 2000)

    } catch (e) {
      setLog([`Error: ${e.message}`])
      setRunning(false)
    }
  }

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 18 }}>Admin — Run Scraper</h2>

      {/* API Status */}
      <Card style={{ marginBottom: 16, borderColor: apiStatus === 'online' ? '#0f5035' : '#5a1010' }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 14 }}>
          <span style={{ fontSize: 22, marginTop: 2 }}>{apiStatus === 'online' ? '🟢' : '🔴'}</span>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 700, fontSize: 15, color: apiStatus === 'online' ? '#2ecc71' : '#e74c3c', marginBottom: 4 }}>
              Railway API {apiStatus === 'online' ? 'Online' : 'Offline'}
            </div>
            {apiStatus === 'online' ? (
              <div style={{ fontSize: 13, color: 'var(--muted)' }}>Railway is running. Ready to scrape.</div>
            ) : (
              <div style={{ fontSize: 13, color: 'var(--muted)', lineHeight: 1.7 }}>
                The Railway backend is not responding. This usually means a deployment is in progress (takes ~60 seconds) or the service crashed.
                <br />
                <span style={{ display: 'block', marginTop: 6, fontSize: 12, color: '#636380' }}>
                  Check your Railway dashboard for deployment status, then hit Retry.
                </span>
              </div>
            )}
          </div>
          <button
            onClick={onCheckApi}
            style={{ background: '#1e1e30', border: '1px solid var(--border)', borderRadius: 8, color: 'var(--muted)', fontSize: 12, padding: '6px 12px', cursor: 'pointer', whiteSpace: 'nowrap', marginTop: 2 }}
          >
            ↻ Retry
          </button>
        </div>
      </Card>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Scraper Settings</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <Input label="Location" value={location} onChange={setLocation} placeholder="Dallas, TX" />
            <Input label="Results Per Category" value={limit} onChange={setLimit} type="number" />

            {/* Category selector */}
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <label style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
                  Categories ({selectedCats.size}/{ALL_CATEGORIES.length} preset{customCats.length > 0 ? ` + ${customCats.length} custom` : ''})
                </label>
                <button onClick={toggleAll} style={{ background: 'none', border: 'none', color: 'var(--accent)', fontSize: 12, cursor: 'pointer', fontWeight: 600 }}>
                  {selectedCats.size === ALL_CATEGORIES.length ? 'Deselect All' : 'Select All'}
                </button>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
                {ALL_CATEGORIES.map(cat => (
                  <label key={cat} style={{ display: 'flex', alignItems: 'center', gap: 7, cursor: 'pointer', fontSize: 13, padding: '5px 8px', borderRadius: 6, background: selectedCats.has(cat) ? '#1a2a1a' : 'var(--surface2)', border: `1px solid ${selectedCats.has(cat) ? '#0f5035' : 'var(--border)'}`, transition: 'all 0.1s' }}>
                    <input type="checkbox" checked={selectedCats.has(cat)} onChange={() => toggleCat(cat)} style={{ accentColor: 'var(--accent)' }} />
                    <span style={{ color: selectedCats.has(cat) ? '#2ecc71' : 'var(--muted)' }}>{cat}</span>
                  </label>
                ))}
              </div>

              {/* Custom vertical input */}
              <div style={{ marginTop: 10 }}>
                <div style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 6 }}>Custom Vertical</div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <input
                    value={customInput}
                    onChange={e => setCustomInput(e.target.value)}
                    onKeyDown={handleCustomKey}
                    placeholder="e.g. Yoga Studios, Pet Groomers..."
                    style={{ flex: 1, background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: '7px 12px', color: 'var(--text)', fontSize: 13, outline: 'none' }}
                  />
                  <Btn size="sm" variant="ghost" onClick={addCustomCat} disabled={!customInput.trim()}>+ Add</Btn>
                </div>
                {customCats.length > 0 && (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 8 }}>
                    {customCats.map(cat => (
                      <span key={cat} style={{ display: 'inline-flex', alignItems: 'center', gap: 6, background: '#1a1a2e', border: '1px solid #3a3a6e', borderRadius: 20, padding: '4px 10px', fontSize: 12, color: '#a78bfa', fontWeight: 600 }}>
                        ✦ {cat}
                        <button onClick={() => removeCustomCat(cat)} style={{ background: 'none', border: 'none', color: '#636380', cursor: 'pointer', padding: 0, fontSize: 14, lineHeight: 1, display: 'flex', alignItems: 'center' }}>×</button>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <label style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', fontSize: 14 }}>
              <input type="checkbox" checked={sendEmails} onChange={e => setSendEmails(e.target.checked)} />
              Auto-send proposals after scrape
            </label>
            <Btn onClick={handleRun} disabled={running || apiStatus !== 'online' || totalCatCount === 0} style={{ width: '100%', justifyContent: 'center', padding: '12px' }}>
              {running ? '⏳ Running...' : `🚀 Run Scraper (${totalCatCount} ${totalCatCount === 1 ? 'category' : 'categories'})`}
            </Btn>
          </div>
        </Card>

        {/* Live log */}
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>
            Live Log {running && <span style={{ color: '#f39c12', fontWeight: 400, fontSize: 11 }}>● streaming</span>}
          </div>
          <div ref={logBoxRef} style={{ background: '#0a0a14', borderRadius: 8, padding: 12, height: 260, overflowY: 'auto', fontFamily: 'monospace', fontSize: 12 }}>
            {log.length === 0
              ? <span style={{ color: 'var(--muted)' }}>Waiting to start...</span>
              : log.map((l, i) => (
                <div key={i} style={{
                  color: l.includes('Error') || l.includes('error') ? '#e74c3c'
                       : l.includes('✓') || l.includes('complete') ? '#2ecc71'
                       : l.includes('→') || l.includes('Scraping') ? '#a78bfa'
                       : l.includes('Found') || l.includes('Saved') ? '#c9a96e'
                       : '#7ec8a0',
                  marginBottom: 3, lineHeight: 1.5,
                }}>
                  {l}
                </div>
              ))}
          </div>
        </Card>
      </div>

      {/* Scrape run history */}
      <Card>
        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Scrape History</div>
        {scrapeRuns.length === 0
          ? <div style={{ color: 'var(--muted)', fontSize: 14 }}>No scrape runs yet.</div>
          : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr>
                {['Started', 'Job Type', 'Location / Label', 'Status', 'Found', 'Emails Sent', 'Duration', ''].map(h => (
                  <th key={h} style={{ padding: '8px 10px', textAlign: 'left', fontSize: 11, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {scrapeRuns.map(r => {
                const duration = r.completed_at ? Math.round((new Date(r.completed_at) - new Date(r.started_at)) / 1000) : null
                const clickable = r.status === 'completed' && r.businesses_found > 0
                const cats = r.categories && r.categories !== 'all'
                  ? r.categories.split(',').map(c => c.trim()).filter(Boolean)
                  : null
                return (
                  <tr key={r.id}
                    onClick={() => clickable && onViewRun(r)}
                    style={{ borderTop: '1px solid var(--border)', cursor: clickable ? 'pointer' : 'default', transition: 'background 0.15s' }}
                    onMouseEnter={e => { if (clickable) e.currentTarget.style.background = 'var(--surface2)' }}
                    onMouseLeave={e => { e.currentTarget.style.background = '' }}
                  >
                    <td style={{ padding: '9px 10px', whiteSpace: 'nowrap' }}>{new Date(r.started_at || r.created_at).toLocaleString()}</td>
                    <td style={{ padding: '9px 10px' }}>
                      <span style={{ background: '#1a2a1a', color: '#2ecc71', border: '1px solid #0f5035', borderRadius: 6, padding: '2px 7px', fontSize: 11, fontWeight: 700 }}>
                        {r.job_type ? r.job_type.replace(/_/g,' ') : 'google maps'}
                      </span>
                    </td>
                    <td style={{ padding: '9px 10px' }}>{r.location}</td>
                    <td style={{ padding: '9px 10px' }}>
                      <span style={{ color: r.status === 'completed' ? '#2ecc71' : r.status === 'failed' ? '#e74c3c' : '#f39c12', fontWeight: 700, fontSize: 12 }}>{(r.status || '').toUpperCase()}</span>
                    </td>
                    <td style={{ padding: '9px 10px', maxWidth: 220 }}>
                      {cats
                        ? <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                            {cats.map(c => <span key={c} style={{ background: '#1a2a1a', color: '#2ecc71', border: '1px solid #0f5035', borderRadius: 6, padding: '2px 7px', fontSize: 11, fontWeight: 600, whiteSpace: 'nowrap' }}>{c}</span>)}
                          </div>
                        : <span style={{ color: 'var(--muted)', fontSize: 12 }}>All categories</span>
                      }
                    </td>
                    <td style={{ padding: '9px 10px' }}>{r.businesses_found ?? '—'}</td>
                    <td style={{ padding: '9px 10px' }}>{r.emails_sent ?? '—'}</td>
                    <td style={{ padding: '9px 10px', color: 'var(--muted)' }}>{duration ? `${duration}s` : '—'}</td>
                    <td style={{ padding: '9px 10px', color: 'var(--accent)', fontSize: 12 }}>{clickable ? '→ View' : ''}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </Card>

    </div>
  )
}

// ── Settings View ─────────────────────────────────────────────
function SettingsView({ userRole, onResetDb }) {
  const [resetStep, setResetStep] = useState(0)

  const handleResetClick = async () => {
    if (resetStep === 0) { setResetStep(1); return }
    if (resetStep === 1) {
      setResetStep(2)
      await onResetDb()
      setResetStep(0)
    }
  }

  return (
    <div>
      <div style={{ fontSize: 22, fontWeight: 800, marginBottom: 24 }}>Settings</div>

      {/* Account info */}
      <Card style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Account</div>
        <div style={{ fontSize: 13, color: 'var(--muted)' }}>Role: <strong style={{ color: userRole === 'admin' ? '#c9a96e' : 'var(--text)', textTransform: 'capitalize' }}>{userRole}</strong></div>
      </Card>

      {/* Danger Zone — admin only */}
      {userRole === 'admin' && (
        <Card style={{ borderColor: resetStep === 1 ? '#e74c3c' : 'var(--border)', background: resetStep === 1 ? '#1a0505' : 'var(--surface)' }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: '#e74c3c', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>☢ Danger Zone</div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
            <div style={{ fontSize: 13, color: 'var(--muted)', lineHeight: 1.6 }}>
              <strong style={{ color: 'var(--text)' }}>Reset Database</strong> — permanently deletes all businesses, emails, follow-ups, feedback, and scrape history. This cannot be undone.
            </div>
            <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexShrink: 0 }}>
              {resetStep === 1 && (
                <span style={{ fontSize: 12, color: '#e74c3c', fontWeight: 700 }}>⚠ Are you sure? Click again to confirm.</span>
              )}
              {resetStep > 0 && (
                <Btn variant="ghost" size="sm" onClick={() => setResetStep(0)} disabled={resetStep === 2}>Cancel</Btn>
              )}
              <Btn variant="danger" size="sm" onClick={handleResetClick} disabled={resetStep === 2}>
                {resetStep === 0 ? '🗑 Reset Database' : resetStep === 1 ? '💀 Yes, Wipe Everything' : '⏳ Resetting...'}
              </Btn>
            </div>
          </div>
        </Card>
      )}
    </div>
  )
}

// ── Job type config ──────────────────────────────────────────
const JOB_TYPES = [
  { id: 'price_scraper',     label: 'Price Scraper',  icon: '💰' },
  { id: 'trivia_scraper',    label: 'Trivia Q&A',     icon: '🧠' },
  { id: 'email_harvester',   label: 'Email Harvest',  icon: '📬' },
  { id: 'content_scraper',   label: 'Content',        icon: '📰' },
  { id: 'paginated_scraper', label: 'Paginated List', icon: '📄' },
]

const RESULT_COLS = {
  google_maps_business: ['name','category','email','phone','website','opportunity_score'],
  price_scraper:        ['name','price','category','source_url'],
  trivia_scraper:       ['question','answer','category','difficulty'],
  email_harvester:      ['email','source_url','label'],
  content_scraper:      ['title','body','link','date','category'],
  paginated_scraper:    ['name','category','source_url','page'],
}

function ResultRow({ row, cols }) {
  const data = { ...row.data, ...row }
  return (
    <tr style={{ borderTop: '1px solid var(--border)' }}>
      {cols.map(col => (
        <td key={col} style={{ padding: '10px 12px', fontSize: 13, maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', verticalAlign: 'top' }}>
          {col === 'website' || col === 'source_url' || col === 'link'
            ? data[col] ? <a href={data[col]} target="_blank" rel="noreferrer" style={{ color: 'var(--accent)', textDecoration: 'none', fontSize: 12 }}>{data[col].replace(/https?:\/\//, '').slice(0,40)}</a> : <span style={{color:'var(--muted)'}}>—</span>
            : col === 'opportunity_score'
              ? data[col] != null ? <ScoreBadge score={data[col]} /> : <span style={{color:'var(--muted)'}}>—</span>
              : <span title={String(data[col] || '')} style={{ color: data[col] ? 'var(--text)' : 'var(--muted)' }}>{data[col] != null ? String(data[col]).slice(0, 120) : '—'}</span>
          }
        </td>
      ))}
    </tr>
  )
}

// ── Universal Scraper View ────────────────────────────────────
function ScraperView({ scrapeRuns, apiStatus, showToast }) {
  const logBoxRef = useRef(null)
  const [jobType, setJobType]     = useState('price_scraper')
  const [running, setRunning]     = useState(false)
  const [log, setLog]             = useState([])
  const [selSession, setSelSession] = useState(null)
  const [results, setResults]     = useState([])
  const [resLoading, setResLoading] = useState(false)

  // Shared fields
  const [urls, setUrls]           = useState('')
  const [category, setCategory]   = useState('')
  const [limit, setLimit]         = useState('100')
  const [label, setLabel]         = useState('')
  // Price
  const [rowSel, setRowSel]       = useState('')
  const [nameSel, setNameSel]     = useState('')
  const [priceSel, setPriceSel]   = useState('')
  // Content
  const [eachItem, setEachItem]   = useState('')
  const [titleSel, setTitleSel]   = useState('h1')
  const [bodySel, setBodySel]     = useState('')
  // Paginated
  const [startUrl, setStartUrl]   = useState('')
  const [itemSel, setItemSel]     = useState('')
  const [nextSel, setNextSel]     = useState('a[rel="next"]')
  const [maxPages, setMaxPages]   = useState('10')
  const [fieldsRaw, setFieldsRaw] = useState('')
  // Trivia
  const [useDefault, setUseDefault] = useState(true)
  // Email
  const [digContact, setDigContact] = useState(true)
  // Destination table
  const [destTable, setDestTable]   = useState('scrape_results')
  const [tables, setTables]         = useState([])
  const [tablesLoading, setTablesLoading] = useState(false)

  // Fetch available Supabase tables for the destination selector
  useEffect(() => {
    setTablesLoading(true)
    supabase.rpc('get_user_tables')
      .then(({ data, error }) => {
        if (data) setTables(data.map(r => r.table_name))
        else if (error) {
          // Fallback: try the Railway API endpoint
          fetch(`${LOCAL_API}/tables`, { signal: AbortSignal.timeout(4000) })
            .then(r => r.json()).then(d => { if (d.tables) setTables(d.tables) })
            .catch(() => {})
        }
        setTablesLoading(false)
      })
  }, [])

  const buildBody = () => {
    const urlList = urls.split('\n').map(u => u.trim()).filter(Boolean)
    const dest = { destination_table: destTable }
    if (jobType === 'price_scraper') {
      const sel = (rowSel || nameSel || priceSel)
        ? { row: rowSel, name: nameSel, price: priceSel }
        : {}
      return { job_type: jobType, urls: urlList, category, selectors: sel, ...dest }
    }
    if (jobType === 'trivia_scraper') {
      return { job_type: jobType, use_default_trivia_sources: useDefault, urls: useDefault ? [] : urlList, category: category || 'trivia', limit: parseInt(limit) || 100, ...dest }
    }
    if (jobType === 'email_harvester') {
      return { job_type: jobType, urls: urlList, label: label || category, dig_contact: digContact, ...dest }
    }
    if (jobType === 'content_scraper') {
      return { job_type: jobType, urls: urlList, each_item: eachItem, selectors: { title: titleSel || 'h1', body: bodySel }, category, ...dest }
    }
    if (jobType === 'paginated_scraper') {
      const fields = {}
      fieldsRaw.split('\n').forEach(line => {
        const [k, v] = line.split(':').map(s => s.trim())
        if (k && v) fields[k] = v
      })
      return { job_type: jobType, start_url: startUrl, item_sel: itemSel, next_sel: nextSel || 'a[rel="next"]', max_pages: parseInt(maxPages) || 10, fields, category, ...dest }
    }
  }

  const handleRun = async () => {
    const body = buildBody()
    if (!body) return
    setRunning(true)
    setLog(['⏳ Starting job...'])
    try {
      const res = await fetch(`${LOCAL_API}/universal-scrape`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json()
      if (data.error) { setLog([`Error: ${data.error}`]); setRunning(false); return }
      setLog([`Started session ${data.run_id}`, '⏳ Polling for updates...'])

      const poll = setInterval(async () => {
        try {
          const sr = await fetch(`${LOCAL_API}/status`, { signal: AbortSignal.timeout(3000) })
          const sd = await sr.json()
          if (sd.log?.length) {
            setLog(sd.log)
            if (logBoxRef.current) logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight
          }
          if (!sd.running) {
            clearInterval(poll)
            setRunning(false)
            showToast('Scrape complete! Click a session below to view results.', 'success')
          }
        } catch { clearInterval(poll); setRunning(false) }
      }, 2000)
    } catch (e) { setLog([`Error: ${e.message}`]); setRunning(false) }
  }

  const loadResults = async (session) => {
    setSelSession(session)
    setResLoading(true)
    const { data } = await supabase.from('scrape_results').select('*')
      .eq('session_id', session.id).order('created_at', { ascending: false }).limit(500)
    setResults(data || [])
    setResLoading(false)
  }

  const cols = RESULT_COLS[selSession?.job_type || jobType] || ['name','category','source_url']

  // Runs for this panel — exclude google_maps (handled in Admin)
  const universalRuns = scrapeRuns.filter(r => r.job_type && r.job_type !== 'google_maps_business')

  return (
    <div>
      <h2 style={{ fontSize: 22, fontWeight: 800, marginBottom: 6 }}>Universal Scraper</h2>
      <p style={{ color: 'var(--muted)', fontSize: 13, marginBottom: 22 }}>
        Scrape anything — prices, trivia, emails, articles, paginated lists. Results save automatically to the database.
      </p>

      {/* API status bar */}
      {apiStatus !== 'online' && (
        <div style={{ background: '#1a0505', border: '1px solid #5a1010', borderRadius: 10, padding: '10px 16px', marginBottom: 16, fontSize: 13, color: '#e74c3c' }}>
          ⚠ Railway API is offline. Start server.py first, then retry.
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>

        {/* ── Left: Job form ── */}
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>Configure Job</div>

          {/* Job type tabs */}
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 18 }}>
            {JOB_TYPES.map(jt => (
              <button key={jt.id} onClick={() => setJobType(jt.id)}
                style={{ padding: '5px 12px', borderRadius: 20, border: `1px solid ${jobType === jt.id ? 'var(--accent)' : 'var(--border)'}`, background: jobType === jt.id ? 'var(--accent)' : 'var(--surface2)', color: jobType === jt.id ? '#fff' : 'var(--muted)', fontSize: 12, fontWeight: 700, cursor: 'pointer' }}>
                {jt.icon} {jt.label}
              </button>
            ))}
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

            {/* ── Price Scraper ── */}
            {jobType === 'price_scraper' && <>
              <Input label="URLs (one per line)" value={urls} onChange={setUrls} placeholder="https://example.com/menu" />
              <Input label="Category" value={category} onChange={setCategory} placeholder="e.g. Craft Beer, Food Menu" />
              <div style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>CSS Selectors (leave blank for auto-detect)</div>
              <Input label="Row container" value={rowSel} onChange={setRowSel} placeholder=".menu-item" />
              <Input label="Name selector" value={nameSel} onChange={setNameSel} placeholder=".item-name" />
              <Input label="Price selector" value={priceSel} onChange={setPriceSel} placeholder=".price" />
            </>}

            {/* ── Trivia ── */}
            {jobType === 'trivia_scraper' && <>
              <label style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', fontSize: 14 }}>
                <input type="checkbox" checked={useDefault} onChange={e => setUseDefault(e.target.checked)} />
                Use Open Trivia DB (free, no URLs needed)
              </label>
              {!useDefault && <Input label="URLs (one per line)" value={urls} onChange={setUrls} placeholder="https://triviasite.com/questions" />}
              <Input label="Category / Tag" value={category} onChange={setCategory} placeholder="Sports, History, etc." />
              <Input label="Max questions" value={limit} onChange={setLimit} type="number" placeholder="100" />
            </>}

            {/* ── Email Harvester ── */}
            {jobType === 'email_harvester' && <>
              <Input label="URLs (one per line)" value={urls} onChange={setUrls} placeholder="https://example.com" />
              <Input label="Label" value={label} onChange={setLabel} placeholder="Dallas Restaurants, Leads Q1..." />
              <label style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', fontSize: 14 }}>
                <input type="checkbox" checked={digContact} onChange={e => setDigContact(e.target.checked)} />
                Also check /contact and /about pages
              </label>
            </>}

            {/* ── Content Scraper ── */}
            {jobType === 'content_scraper' && <>
              <Input label="URLs (one per line)" value={urls} onChange={setUrls} placeholder="https://example.com/blog" />
              <Input label="Category" value={category} onChange={setCategory} placeholder="News, Blog Posts..." />
              <Input label="Repeating item selector" value={eachItem} onChange={setEachItem} placeholder=".article, .post-card (blank = whole page)" />
              <Input label="Title selector" value={titleSel} onChange={setTitleSel} placeholder="h1, h2.title" />
              <Input label="Body selector" value={bodySel} onChange={setBodySel} placeholder=".article-body, p.summary" />
            </>}

            {/* ── Paginated ── */}
            {jobType === 'paginated_scraper' && <>
              <Input label="Start URL" value={startUrl} onChange={setStartUrl} placeholder="https://example.com/listings" />
              <Input label="Item selector" value={itemSel} onChange={setItemSel} placeholder=".listing-card" />
              <Input label="Next page selector" value={nextSel} onChange={setNextSel} placeholder='a[rel="next"]' />
              <Input label="Max pages" value={maxPages} onChange={setMaxPages} type="number" />
              <Input label="Category" value={category} onChange={setCategory} placeholder="Jobs, Listings..." />
              <div>
                <label style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>Fields (name:selector, one per line)</label>
                <textarea value={fieldsRaw} onChange={e => setFieldsRaw(e.target.value)} placeholder={'title:.job-title\ncompany:.company-name\nlocation:.location'}
                  style={{ width: '100%', background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 8, padding: 10, color: 'var(--text)', fontSize: 13, resize: 'vertical', minHeight: 80, outline: 'none', marginTop: 6 }} />
              </div>
            </>}

            {/* Destination table selector */}
            <div style={{ borderTop: '1px solid var(--border)', paddingTop: 14, marginTop: 4 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                <label style={{ fontSize: 12, color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
                  Save Results To
                </label>
                {tablesLoading && <span style={{ fontSize: 11, color: 'var(--muted)' }}>Loading tables...</span>}
              </div>
              <select value={destTable} onChange={e => setDestTable(e.target.value)}
                style={{ width: '100%', background: 'var(--surface2)', border: `1px solid ${destTable !== 'scrape_results' ? 'var(--accent)' : 'var(--border)'}`, borderRadius: 8, padding: '9px 12px', color: destTable !== 'scrape_results' ? 'var(--accent)' : 'var(--text)', fontSize: 14, outline: 'none', fontWeight: destTable !== 'scrape_results' ? 700 : 400 }}>
                <option value="scrape_results">scrape_results (default — always saved here)</option>
                {tables.filter(t => t !== 'scrape_results').map(t => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              {destTable !== 'scrape_results' && (
                <div style={{ fontSize: 12, color: '#c9a96e', marginTop: 6 }}>
                  Results will be saved to both <strong>scrape_results</strong> (audit trail) and <strong>{destTable}</strong>. Only fields matching that table's columns will be inserted.
                </div>
              )}
            </div>

            <Btn onClick={handleRun} disabled={running || apiStatus !== 'online'} style={{ width: '100%', justifyContent: 'center', padding: '12px', marginTop: 4 }}>
              {running ? '⏳ Running...' : `🚀 Launch → ${destTable}`}
            </Btn>
          </div>
        </Card>

        {/* ── Right: Live log ── */}
        <Card>
          <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>
            Live Log {running && <span style={{ color: '#f39c12', fontWeight: 400, fontSize: 11 }}>● streaming</span>}
          </div>
          <div ref={logBoxRef} style={{ background: '#0a0a14', borderRadius: 8, padding: 12, height: 340, overflowY: 'auto', fontFamily: 'monospace', fontSize: 12 }}>
            {log.length === 0
              ? <span style={{ color: 'var(--muted)' }}>Waiting to start...</span>
              : log.map((l, i) => (
                <div key={i} style={{
                  color: l.includes('Error') || l.includes('error') || l.includes('[!]') ? '#e74c3c'
                       : l.includes('✓') || l.includes('complete') || l.includes('Done') ? '#2ecc71'
                       : l.includes('→') || l.includes('JOB:') ? '#a78bfa'
                       : l.includes('Found') || l.includes('Saved') || l.includes('rows') ? '#c9a96e'
                       : '#7ec8a0',
                  marginBottom: 3, lineHeight: 1.5,
                }}>{l}</div>
              ))}
          </div>
        </Card>
      </div>

      {/* ── Session History ── */}
      <Card style={{ marginBottom: selSession ? 16 : 0 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 14 }}>
          Scrape Sessions — click to view results
        </div>
        {universalRuns.length === 0
          ? <div style={{ color: 'var(--muted)', fontSize: 14 }}>No universal scrape jobs run yet. Launch one above.</div>
          : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr>
                  {['Started', 'Job Type', 'Category', 'Status', 'Results', 'Duration', ''].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', fontSize: 11, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, whiteSpace: 'nowrap' }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {universalRuns.map(r => {
                  const duration = r.completed_at ? Math.round((new Date(r.completed_at) - new Date(r.started_at || r.created_at)) / 1000) : null
                  const jt = JOB_TYPES.find(j => j.id === r.job_type)
                  const isSelected = selSession?.id === r.id
                  return (
                    <tr key={r.id} onClick={() => loadResults(r)}
                      style={{ borderTop: '1px solid var(--border)', cursor: 'pointer', background: isSelected ? 'var(--surface2)' : '', transition: 'background 0.15s' }}
                      onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = 'var(--surface2)' }}
                      onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = '' }}>
                      <td style={{ padding: '9px 10px', whiteSpace: 'nowrap' }}>{new Date(r.started_at || r.created_at).toLocaleString()}</td>
                      <td style={{ padding: '9px 10px' }}>
                        <span style={{ background: '#1a2a1a', color: '#2ecc71', border: '1px solid #0f5035', borderRadius: 6, padding: '2px 8px', fontSize: 11, fontWeight: 700 }}>
                          {jt ? `${jt.icon} ${jt.label}` : r.job_type}
                        </span>
                      </td>
                      <td style={{ padding: '9px 10px', color: 'var(--muted)' }}>{r.categories || '—'}</td>
                      <td style={{ padding: '9px 10px' }}>
                        <span style={{ color: r.status === 'completed' ? '#2ecc71' : r.status === 'failed' ? '#e74c3c' : '#f39c12', fontWeight: 700, fontSize: 12 }}>{r.status?.toUpperCase()}</span>
                      </td>
                      <td style={{ padding: '9px 10px', fontWeight: 700, color: '#c9a96e' }}>{r.result_count ?? r.businesses_found ?? '—'}</td>
                      <td style={{ padding: '9px 10px', color: 'var(--muted)' }}>{duration ? `${duration}s` : '—'}</td>
                      <td style={{ padding: '9px 10px', color: 'var(--accent)', fontSize: 12, whiteSpace: 'nowrap' }}>
                        {isSelected ? '▼ Viewing' : '→ View'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* ── Results Table ── */}
      {selSession && (
        <Card>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: 1 }}>
              Results — {selSession.categories || selSession.job_type}
              <span style={{ color: '#c9a96e', marginLeft: 10, fontWeight: 400, fontSize: 12 }}>({results.length} rows)</span>
            </div>
            <Btn size="sm" variant="ghost" onClick={() => { setSelSession(null); setResults([]) }}>✕ Close</Btn>
          </div>

          {resLoading ? (
            <div style={{ color: 'var(--muted)', fontSize: 14, padding: '32px 0', textAlign: 'center' }}>Loading results...</div>
          ) : results.length === 0 ? (
            <div style={{ color: 'var(--muted)', fontSize: 14, padding: '16px 0' }}>No results found for this session. The job may still be running.</div>
          ) : (
            <div style={{ overflowX: 'auto', borderRadius: 8, border: '1px solid var(--border)' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', background: 'var(--surface)', fontSize: 13 }}>
                <thead style={{ background: 'var(--surface2)' }}>
                  <tr>
                    {cols.map(col => (
                      <th key={col} style={{ padding: '10px 12px', textAlign: 'left', fontSize: 11, color: 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, whiteSpace: 'nowrap' }}>
                        {col.replace(/_/g, ' ')}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, i) => <ResultRow key={row.id || i} row={row} cols={cols} />)}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      )}
    </div>
  )
}

// ── Main App ─────────────────────────────────────────────────
export default function App() {
  const isMobile = useIsMobile()
  const [tab, setTab]               = useState('dashboard')
  const [businesses, setBusinesses] = useState([])
  const [emailLogs, setEmailLogs]   = useState([])
  const [followUps, setFollowUps]   = useState([])
  const [feedback, setFeedback]     = useState([])
  const [scrapeRuns, setScrapeRuns] = useState([])
  const [loading, setLoading]       = useState(true)
  const [selectedBiz, setSelectedBiz] = useState(null)
  const [apiStatus, setApiStatus]   = useState('unknown')
  const [toast, setToast]           = useState(null)
  const [runFilter, setRunFilter]   = useState(null)

  // Auth state
  const [session, setSession]           = useState(null)
  const [userRole, setUserRole]         = useState('user')
  const [authLoading, setAuthLoading]   = useState(true)
  const [needsPasswordSet, setNeedsPasswordSet] = useState(false)
  const [showChangePassword, setShowChangePassword] = useState(false)

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }

  // ── Auth ───────────────────────────────────────────────────
  const fetchProfile = useCallback(async (userId) => {
    const { data } = await supabase.from('profiles').select('role').eq('id', userId).single()
    setUserRole(data?.role || 'user')
  }, [])

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session)
      if (session) fetchProfile(session.user.id)
      setAuthLoading(false)
    })
    const { data: { subscription } } = supabase.auth.onAuthStateChange((event, session) => {
      if (event === 'PASSWORD_RECOVERY') {
        setSession(session)
        setNeedsPasswordSet(true)
        setAuthLoading(false)
      } else {
        setNeedsPasswordSet(false)
        setSession(session)
        if (session) fetchProfile(session.user.id)
        else { setUserRole('user'); setAuthLoading(false) }
      }
    })
    return () => subscription.unsubscribe()
  }, [fetchProfile])

  const handleLogout = async () => {
    await supabase.auth.signOut()
    setSession(null)
    setUserRole('user')
  }

  // Load all data from Supabase
  // silent=true skips the loading spinner so mounted children aren't unmounted
  const loadData = useCallback(async (silent = false) => {
    if (!silent) setLoading(true)
    const [b, e, f, fb, sr] = await Promise.all([
      supabase.from('businesses').select('*').order('opportunity_score', { ascending: false }),
      supabase.from('email_logs').select('*').order('sent_at', { ascending: false }),
      supabase.from('follow_ups').select('*').order('scheduled_for', { ascending: true }),
      supabase.from('feedback').select('*').order('received_at', { ascending: false }),
      supabase.from('scrape_runs').select('*').order('started_at', { ascending: false }),
    ])
    if (b.data)  setBusinesses(b.data)
    if (e.data)  setEmailLogs(e.data)
    if (f.data)  setFollowUps(f.data)
    if (fb.data) setFeedback(fb.data)
    if (sr.data) setScrapeRuns(sr.data)
    if (!silent) setLoading(false)
  }, [])

  // Check local API
  const checkApi = useCallback(async () => {
    try {
      const res = await fetch(`${LOCAL_API}/health`, { signal: AbortSignal.timeout(3000) })
      setApiStatus(res.ok ? 'online' : 'offline')
    } catch {
      setApiStatus('offline')
    }
  }, [])

  useEffect(() => { loadData(); checkApi() }, [loadData, checkApi])
  useEffect(() => { const t = setInterval(checkApi, 15000); return () => clearInterval(t) }, [checkApi])

  // Auto-mark follow_up_due
  useEffect(() => {
    const overdueIds = followUps
      .filter(f => f.status === 'pending' && daysUntil(f.scheduled_for) <= 0)
      .map(f => f.business_id)
    if (overdueIds.length === 0) return
    businesses.forEach(async b => {
      if (overdueIds.includes(b.id) && b.status === 'emailed') {
        await supabase.from('businesses').update({ status: 'follow_up_due' }).eq('id', b.id)
      }
    })
  }, [followUps, businesses])

  const handleStatusChange = async (id, status) => {
    await supabase.from('businesses').update({ status }).eq('id', id)
    setBusinesses(prev => prev.map(b => b.id === id ? { ...b, status } : b))
    if (selectedBiz?.id === id) setSelectedBiz(prev => ({ ...prev, status }))
    showToast(`Status updated to ${STATUS_META[status]?.label}`)
  }

  const handleAddNote = async (id, notes) => {
    await supabase.from('businesses').update({ notes }).eq('id', id)
    setBusinesses(prev => prev.map(b => b.id === id ? { ...b, notes } : b))
    showToast('Note saved')
  }

  const handleAddFeedback = async (businessId, message, sentiment) => {
    const { data } = await supabase.from('feedback').insert({ business_id: businessId, message, sentiment }).select()
    if (data) {
      setFeedback(prev => [data[0], ...prev])
      // Auto-update status to replied if positive
      if (sentiment === 'positive') handleStatusChange(businessId, 'replied')
      showToast('Feedback logged')
    }
  }

  const handleScheduleFollowUp = async (businessId, days, followUpNumber) => {
    const scheduled = new Date()
    scheduled.setDate(scheduled.getDate() + days)
    const { data } = await supabase.from('follow_ups').insert({
      business_id: businessId,
      scheduled_for: scheduled.toISOString(),
      follow_up_number: followUpNumber,
    }).select()
    if (data) {
      setFollowUps(prev => [...prev, data[0]])
      showToast(`Follow-up scheduled for ${days} days from now`)
    }
  }

  const handleMarkSent = async (followUpId) => {
    await supabase.from('follow_ups').update({ status: 'sent', sent_at: new Date().toISOString() }).eq('id', followUpId)
    setFollowUps(prev => prev.map(f => f.id === followUpId ? { ...f, status: 'sent', sent_at: new Date().toISOString() } : f))
    showToast('Follow-up marked as sent')
  }

  const handleSendFollowUp = async (followUp, biz) => {
    try {
      const res = await fetch(`${LOCAL_API}/send-followup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ business_id: biz.id, follow_up_id: followUp.id, to_email: biz.email, business_name: biz.name }),
      })
      if (res.ok) {
        handleMarkSent(followUp.id)
        handleStatusChange(biz.id, 'follow_up_sent')
        showToast(`Follow-up sent to ${biz.name}`)
      } else {
        showToast('Failed to send follow-up', 'error')
      }
    } catch {
      showToast('Railway API offline — check Settings tab', 'error')
    }
  }

  const handleViewRun = (run) => {
    setRunFilter(run)
    setTab('businesses')
  }

  const handleResetDb = async () => {
    try {
      const res = await fetch(`${LOCAL_API}/reset-db`, { method: 'POST' })
      const data = await res.json()
      if (data.success) {
        await loadData()
        showToast('Database wiped. Fresh start.', 'success')
      } else {
        showToast(data.error || 'Reset failed', 'error')
      }
    } catch {
      showToast('Railway API offline — check Settings tab', 'error')
    }
  }

  const handleRunScraper = async (opts) => {
    try {
      const res = await fetch(`${LOCAL_API}/scrape`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(opts),
      })
      const data = await res.json()
      // Don't loadData() here — scrape just started, nothing to refresh yet.
      // AdminView's polling will call onScrapeComplete when done.
      return data
    } catch (e) {
      throw new Error('Could not reach the Railway API. Check the Settings tab for connection status.')
    }
  }

  const NAV = [
    { id: 'dashboard',  label: 'Dashboard',    icon: '📊' },
    { id: 'businesses', label: 'Businesses',    icon: '🏢' },
    { id: 'emails',     label: 'Emails',        icon: '📧' },
    { id: 'followups',  label: 'Follow-Ups',    icon: '🔁' },
    { id: 'feedback',   label: 'Feedback',      icon: '💬' },
    { id: 'scraper',    label: 'Scraper',        icon: '🕷️' },
    { id: 'admin',      label: 'Admin / Run',   icon: '🛠️' },
    { id: 'settings',   label: 'Settings',       icon: '⚙️' },
  ]

  const dueCount = followUps.filter(f => f.status === 'pending' && daysUntil(f.scheduled_for) <= 0).length

  // ── Auth gate ──────────────────────────────────────────────
  if (authLoading) {
    return (
      <div style={{ minHeight: '100vh', background: 'var(--bg)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ color: 'var(--muted)', fontSize: 14 }}>Loading...</div>
      </div>
    )
  }
  if (needsPasswordSet) return <SetPasswordScreen onDone={() => setNeedsPasswordSet(false)} />
  if (!session) return <LoginScreen />

  return (
    <div style={{ display: 'flex', minHeight: '100vh', flexDirection: isMobile ? 'column' : 'row' }}>

      {/* ── Desktop Sidebar ── */}
      {!isMobile && (
        <aside style={{ width: 220, background: 'var(--surface)', borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column', flexShrink: 0, position: 'sticky', top: 0, height: '100vh' }}>
          <div style={{ padding: '22px 20px 16px', borderBottom: '1px solid var(--border)' }}>
            <div style={{ fontWeight: 800, fontSize: 16, color: 'var(--text)', letterSpacing: -0.3 }}>🔨 Rebuild Digital</div>
            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 3 }}>Outreach Tracker</div>
          </div>
          <nav style={{ flex: 1, padding: '10px 10px' }}>
            {NAV.map(n => (
              <button key={n.id} onClick={() => setTab(n.id)}
                style={{ width: '100%', display: 'flex', alignItems: 'center', gap: 10, padding: '9px 12px', borderRadius: 8, border: 'none', background: tab === n.id ? 'var(--accent)' : 'transparent', color: tab === n.id ? '#fff' : 'var(--muted)', fontSize: 14, fontWeight: tab === n.id ? 700 : 400, cursor: 'pointer', marginBottom: 2, textAlign: 'left', position: 'relative' }}>
                <span>{n.icon}</span>
                <span>{n.label}</span>
                {n.id === 'followups' && dueCount > 0 && (
                  <span style={{ marginLeft: 'auto', background: '#e74c3c', color: '#fff', borderRadius: 10, padding: '1px 7px', fontSize: 11, fontWeight: 800 }}>{dueCount}</span>
                )}
                {n.id === 'settings' && (
                  <span style={{ marginLeft: 'auto', width: 8, height: 8, borderRadius: '50%', background: apiStatus === 'online' ? '#2ecc71' : '#e74c3c', flexShrink: 0 }} />
                )}
              </button>
            ))}
          </nav>
          {/* User info + logout */}
          <div style={{ padding: '12px 14px', borderTop: '1px solid var(--border)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
              <div style={{ width: 28, height: 28, borderRadius: '50%', background: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, fontWeight: 800, flexShrink: 0 }}>
                {session.user.email?.[0]?.toUpperCase() || '?'}
              </div>
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{session.user.email}</div>
                <div style={{ fontSize: 10, color: userRole === 'admin' ? '#c9a96e' : 'var(--muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                  {userRole === 'admin' ? '⭐ Admin' : 'User'}
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              <button onClick={() => setShowChangePassword(true)}
                style={{ flex: 1, background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 7, color: 'var(--muted)', fontSize: 11, padding: '6px', cursor: 'pointer', fontWeight: 600 }}>
                🔑 Password
              </button>
              <button onClick={handleLogout}
                style={{ flex: 1, background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 7, color: 'var(--muted)', fontSize: 11, padding: '6px', cursor: 'pointer', fontWeight: 600 }}>
                Sign Out
              </button>
            </div>
            <div style={{ fontSize: 10, color: '#636380', marginTop: 8, textAlign: 'center' }}>
              {businesses.length} businesses · {emailLogs.length} emails
            </div>
          </div>
        </aside>
      )}

      {/* ── Mobile Top Bar ── */}
      {isMobile && (
        <header style={{ background: 'var(--surface)', borderBottom: '1px solid var(--border)', padding: '12px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', position: 'sticky', top: 0, zIndex: 100 }}>
          <div>
            <div style={{ fontWeight: 800, fontSize: 15, color: 'var(--text)' }}>🔨 Rebuild Digital</div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {apiStatus === 'online' && <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#2ecc71', display: 'inline-block' }} />}
            <button onClick={() => setShowChangePassword(true)}
              style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, color: 'var(--muted)', fontSize: 11, padding: '5px 10px', cursor: 'pointer', fontWeight: 600 }}>
              🔑
            </button>
            <button onClick={handleLogout}
              style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, color: 'var(--muted)', fontSize: 11, padding: '5px 10px', cursor: 'pointer', fontWeight: 600 }}>
              Out
            </button>
          </div>
        </header>
      )}

      {/* Main */}
      <main style={{ flex: 1, padding: isMobile ? '18px 14px' : '28px 32px', overflowY: 'auto', minWidth: 0, paddingBottom: isMobile ? 80 : undefined }}>
        {loading
          ? <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 300, color: 'var(--muted)' }}>Loading...</div>
          : (
          <>
            {tab === 'dashboard'  && <Dashboard businesses={businesses} emailLogs={emailLogs} followUps={followUps} feedback={feedback} scrapeRuns={scrapeRuns} />}
            {tab === 'businesses' && <BusinessesView businesses={businesses} onStatusChange={handleStatusChange} onSelect={setSelectedBiz} runFilter={runFilter} onClearRunFilter={() => setRunFilter(null)} />}
            {tab === 'emails'     && <EmailsView emailLogs={emailLogs} businesses={businesses} />}
            {tab === 'followups'  && <FollowUpsView followUps={followUps} businesses={businesses} onMarkSent={handleMarkSent} onSendFollowUp={handleSendFollowUp} />}
            {tab === 'feedback'   && <FeedbackView feedback={feedback} businesses={businesses} />}
            {tab === 'scraper'    && <ScraperView scrapeRuns={scrapeRuns} apiStatus={apiStatus} showToast={showToast} />}
            {tab === 'admin'      && <AdminView scrapeRuns={scrapeRuns} onRunScraper={handleRunScraper} apiStatus={apiStatus} onCheckApi={checkApi} onViewRun={handleViewRun} onResetDb={handleResetDb} userRole={userRole} onScrapeComplete={() => loadData(true)} />}
            {tab === 'settings'   && <SettingsView userRole={userRole} onResetDb={handleResetDb} />}
          </>
        )}
      </main>

      {/* Business detail panel */}
      {selectedBiz && (
        <BusinessModal
          biz={selectedBiz}
          emailLogs={emailLogs}
          followUps={followUps}
          feedback={feedback}
          onClose={() => setSelectedBiz(null)}
          onStatusChange={handleStatusChange}
          onAddNote={handleAddNote}
          onAddFeedback={handleAddFeedback}
          onScheduleFollowUp={handleScheduleFollowUp}
          apiStatus={apiStatus}
          onRefresh={loadData}
          showToast={showToast}
        />
      )}

      {/* Change password modal */}
      {showChangePassword && <ChangePasswordModal onClose={() => setShowChangePassword(false)} />}

      {/* ── Mobile Bottom Nav ── */}
      {isMobile && (
        <nav style={{ position: 'fixed', bottom: 0, left: 0, right: 0, background: 'var(--surface)', borderTop: '1px solid var(--border)', display: 'flex', zIndex: 200, paddingBottom: 'env(safe-area-inset-bottom)' }}>
          {NAV.map(n => (
            <button key={n.id} onClick={() => setTab(n.id)}
              style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2, padding: '8px 2px 6px', border: 'none', background: 'transparent', color: tab === n.id ? 'var(--accent)' : 'var(--muted)', fontSize: 10, fontWeight: tab === n.id ? 700 : 400, cursor: 'pointer', position: 'relative' }}>
              <span style={{ fontSize: 18 }}>{n.icon}</span>
              <span style={{ fontSize: 9, letterSpacing: -0.2 }}>{n.id === 'followups' ? 'Follow-Ups' : n.id === 'admin' ? 'Admin' : n.label}</span>
              {n.id === 'followups' && dueCount > 0 && (
                <span style={{ position: 'absolute', top: 4, right: '50%', transform: 'translateX(10px)', background: '#e74c3c', color: '#fff', borderRadius: 8, padding: '1px 5px', fontSize: 9, fontWeight: 800 }}>{dueCount}</span>
              )}
              {tab === n.id && <span style={{ position: 'absolute', bottom: 0, left: '20%', right: '20%', height: 2, background: 'var(--accent)', borderRadius: 1 }} />}
            </button>
          ))}
        </nav>
      )}

      {/* Toast */}
      {toast && (
        <div style={{ position: 'fixed', bottom: isMobile ? 76 : 24, right: isMobile ? 12 : 24, left: isMobile ? 12 : 'auto', background: toast.type === 'error' ? '#3d0a0a' : '#0a3020', border: `1px solid ${toast.type === 'error' ? '#e74c3c' : '#2ecc71'}`, color: toast.type === 'error' ? '#e74c3c' : '#2ecc71', padding: '12px 20px', borderRadius: 10, fontSize: 14, fontWeight: 600, zIndex: 9999, boxShadow: '0 4px 20px rgba(0,0,0,0.4)', textAlign: isMobile ? 'center' : 'left' }}>
          {toast.type === 'error' ? '✗' : '✓'} {toast.msg}
        </div>
      )}
    </div>
  )
}
