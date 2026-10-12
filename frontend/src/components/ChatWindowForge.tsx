import './forge.css'
import 'katex/dist/katex.min.css'
import remarkGfm from 'remark-gfm'
import remarkMath from 'remark-math'
import rehypeKatex from 'rehype-katex'
import ReactMarkdown from 'react-markdown'
import { useState, useRef, useEffect, useCallback } from 'react'
import RiskHeatmap, { type RiskHeatmapDisplay } from './RiskHeatmap'

type Display  = RiskHeatmapDisplay
type JobStatus = 'succeeded' | 'failed' | 'unknown'
type Message  = { role: 'user' | 'assistant'; content: string; displays?: Display[]; job?: JobStatus }
type Session  = { id: string; startedAt: number; messages: Message[] }
type NodeInfo = { instance: string; up: boolean }
type NodeStatus = Record<string, NodeInfo>
type ActiveJob  = { id: string; type: string; label: string }
type JobNotification = { id: string; status: JobStatus; content: string; displays: Display[] }

// ── External dashboards (right-hand link rail) ───────────────────────────────

const svgProps = {
  width: 20, height: 20, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor',
  strokeWidth: 1.6, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const,
}

const DASHBOARD_LINKS = [
  {
    label: 'SPARK',
    title: 'Spark master UI: workers, running and completed applications',
    href: 'http://192.168.1.250:8080/',
    icon: (  // spark / star burst
      <svg {...svgProps}>
        <path d="M12 2v6M12 16v6M2 12h6M16 12h6M4.9 4.9l4.2 4.2M14.9 14.9l4.2 4.2M4.9 19.1l4.2-4.2M14.9 9.1l4.2-4.2" />
      </svg>
    ),
  },
  {
    label: 'GRAFANA',
    title: 'Grafana: Kitsune cluster monitor (live node metrics)',
    href: 'http://localhost:3000/d/adbpxfp/kitsune-cluster-monitor?orgId=1&from=now-15m&to=now&timezone=browser&refresh=5s',
    icon: (  // bar chart
      <svg {...svgProps}>
        <path d="M3 21h18" />
        <path d="M6 17v-5M11 17V7M16 17v-8M20 17v-3" />
      </svg>
    ),
  },
  {
    label: 'MLFLOW',
    title: 'MLflow: agent traces and per-query metrics',
    href: 'http://localhost:5000/#/experiments/4/overview',
    icon: (  // flask
      <svg {...svgProps}>
        <path d="M9 3h6M10 3v6l-5 9a2 2 0 0 0 1.8 3h10.4a2 2 0 0 0 1.8-3l-5-9V3" />
        <path d="M7.5 15h9" />
      </svg>
    ),
  },
]

// ── localStorage helpers ──────────────────────────────────────────────────────

function loadSessions(): Session[] {
  try { return JSON.parse(localStorage.getItem('forge-sessions') ?? '[]') }
  catch { return [] }
}
function persistSessions(s: Session[]) {
  localStorage.setItem('forge-sessions', JSON.stringify(s))
}
function freshSession(): Session {
  return { id: crypto.randomUUID(), startedAt: Date.now(), messages: [] }
}
function sessionLabel(s: Session): string {
  const first = s.messages.find(m => m.role === 'user')
  if (!first) return 'Empty session'
  return first.content.length > 30 ? first.content.slice(0, 30) + '…' : first.content
}
function sessionDate(s: Session): string {
  return new Date(s.startedAt).toLocaleString([], {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  })
}

// ── Component ────────────────────────────────────────────────────────────────

export default function ChatWindowForge() {
  // Initialise sessions; create one if storage is empty
  const [sessions, setSessions] = useState<Session[]>(() => {
    const saved = loadSessions()
    if (saved.length > 0) return saved
    const s = freshSession()
    persistSessions([s])
    localStorage.setItem('forge-active-session', s.id)
    return [s]
  })

  const [activeId, setActiveId] = useState<string>(() => {
    const id    = localStorage.getItem('forge-active-session') ?? ''
    const saved = loadSessions()
    if (saved.find(s => s.id === id)) return id
    const last  = saved[saved.length - 1]
    if (last) { localStorage.setItem('forge-active-session', last.id); return last.id }
    return ''
  })

  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [input,       setInput]       = useState('')
  const [isLoading,   setIsLoading]   = useState(false)
  const [nodeStatus,  setNodeStatus]  = useState<NodeStatus>({})
  const [activeJobs,  setActiveJobs]  = useState<ActiveJob[]>([])
  const pendingRef = useRef(false)
  const activeIdRef = useRef(activeId)
  const bottomRef  = useRef<HTMLDivElement>(null)

  // Derived: messages for the active session
  const messages = sessions.find(s => s.id === activeId)?.messages ?? []

  // Persist sessions whenever they change
  useEffect(() => { persistSessions(sessions) }, [sessions])
  useEffect(() => {
    localStorage.setItem('forge-active-session', activeId)
    activeIdRef.current = activeId
  }, [activeId])

  // Auto-scroll
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // ── Session management ────────────────────────────────────────────────────

  function updateMessages(updater: (prev: Message[]) => Message[]) {
    setSessions(prev => prev.map(s =>
      s.id === activeId ? { ...s, messages: updater(s.messages) } : s
    ))
  }

  function newSession() {
    const s = freshSession()
    setSessions(prev => [...prev, s])
    setActiveId(s.id)
  }

  function selectSession(id: string) {
    setActiveId(id)
    setSidebarOpen(false)
  }

  function deleteSession(e: React.MouseEvent, id: string) {
    e.stopPropagation()
    setSessions(prev => {
      const updated = prev.filter(s => s.id !== id)
      if (updated.length === 0) {
        const s = freshSession()
        setActiveId(s.id)
        return [s]
      }
      if (id === activeId) setActiveId(updated[updated.length - 1].id)
      return updated
    })
  }

  // ── Node status polling ───────────────────────────────────────────────────

  const fetchNodeStatus = useCallback(async () => {
    try {
      const res = await fetch('/nodes')
      if (res.ok) setNodeStatus(await res.json())
    } catch { /* keep last known state */ }
  }, [])

  useEffect(() => {
    fetchNodeStatus()
    const id = setInterval(fetchNodeStatus, 5000)
    return () => clearInterval(id)
  }, [fetchNodeStatus])

  // ── Background job polling ────────────────────────────────────────────────
  // Long jobs (e.g. RISK simulations) run detached; the backend queues an alert
  // when one finishes, which is posted into whichever session is open.

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/jobs')
      if (!res.ok) return
      const data: { active: ActiveJob[]; notifications: JobNotification[] } = await res.json()
      setActiveJobs(data.active)
      if (data.notifications.length === 0) return

      const notices: Message[] = data.notifications.map(n => ({
        role: 'assistant', content: n.content, displays: n.displays, job: n.status,
      }))
      setSessions(prev => prev.map(s =>
        s.id === activeIdRef.current ? { ...s, messages: [...s.messages, ...notices] } : s
      ))
      if (document.hidden && 'Notification' in window && Notification.permission === 'granted') {
        for (const n of data.notifications) {
          new Notification('Kitsune: job ' + n.status, { body: n.content.replace(/[*`]/g, '').slice(0, 200) })
        }
      }
    } catch { /* keep last known state */ }
  }, [])

  useEffect(() => {
    fetchJobs()
    const id = setInterval(fetchJobs, 5000)
    return () => clearInterval(id)
  }, [fetchJobs])

  // ── Chat ──────────────────────────────────────────────────────────────────

  async function handleSend() {
    if (!input.trim() || pendingRef.current) return
    pendingRef.current = true

    const userMessage: Message = { role: 'user', content: input }
    updateMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      const res = await fetch('/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: userMessage.content }),
      })
      if (res.status === 409) {
        updateMessages(prev => [...prev, { role: 'assistant', content: 'ERR: agent busy — previous request still processing.' }])
        return
      }
      const data = await res.json()
      updateMessages(prev => [...prev, { role: 'assistant', content: data.reply, displays: data.displays ?? [] }])
      fetchJobs()  // show a newly launched job's badge straight away
      if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission()  // for alerts when a job finishes while the tab is hidden
      }
    } catch {
      updateMessages(prev => [...prev, { role: 'assistant', content: 'ERR: connection refused — server unreachable.' }])
    } finally {
      pendingRef.current = false
      setIsLoading(false)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  // ── Render ────────────────────────────────────────────────────────────────

  const sortedSessions = [...sessions].reverse()

  return (
    <div className="forge-container">

      {/* ── Sidebar ── */}
      <div className={`forge-sidebar ${sidebarOpen ? 'open' : 'closed'}`}>
        <div className="forge-sidebar-header">
          <span className="forge-sidebar-title">SESSIONS</span>
          <button className="forge-new-btn" onClick={newSession}>+ NEW</button>
        </div>
        <div className="forge-session-list">
          {sortedSessions.map(s => (
            <div
              key={s.id}
              className={`forge-session-item ${s.id === activeId ? 'active' : ''}`}
              onClick={() => selectSession(s.id)}
            >
              <div className="forge-session-info">
                <div className="forge-session-label">{sessionLabel(s)}</div>
                <div className="forge-session-date">{sessionDate(s)}</div>
              </div>
              <button
                className="forge-session-del"
                onClick={e => deleteSession(e, s.id)}
                title="Delete session"
              >×</button>
            </div>
          ))}
        </div>
      </div>

      {/* ── Main ── */}
      <div className="forge-main">
        <div className="forge-header">
          <button
            className="forge-sidebar-toggle"
            onClick={() => setSidebarOpen(o => !o)}
            title="Toggle session history"
          >≡</button>
          <div className="forge-status-dot" />
          <span className="forge-header-title">KITSUNE CLUSTER MANAGER</span>
          <div className="forge-node-badges">
            {activeJobs.map(j => (
              <span key={j.id} className="forge-node-badge job" title={j.label}>
                <span className="forge-node-led job" />
                JOB RUNNING
              </span>
            ))}
            {Object.keys(nodeStatus).length === 0
              ? <span className="forge-node-badge unknown"><span className="forge-node-led unknown" />CONNECTING</span>
              : Object.entries(nodeStatus).map(([name, info]) => {
                  const state = info.up ? 'up' : 'down'
                  return (
                    <span key={name} className={`forge-node-badge ${state}`}>
                      <span className={`forge-node-led ${state}`} />
                      {name}
                    </span>
                  )
                })
            }
          </div>
        </div>

        <div className="forge-messages">
          {messages.length === 0 && (
            <div className="forge-empty">
              <div className="forge-empty-icon">⬡</div>
              <div>CLUSTER MANAGER READY</div>
              <div className="forge-empty-sub">run get_cluster_summary() to begin</div>
            </div>
          )}

          {messages.map((msg, i) => (
            <div key={i} className={`forge-msg ${msg.role}${msg.job ? ` job ${msg.job}` : ''}`}>
              <div className="forge-label">{msg.role === 'user' ? 'OPERATOR' : msg.job ? `JOB ${msg.job}` : 'AGENT'}</div>
              <div className="forge-bubble">
                {msg.role === 'assistant'
                  ? <ReactMarkdown remarkPlugins={[remarkMath, remarkGfm]} rehypePlugins={[rehypeKatex]}>
                      {msg.content}
                    </ReactMarkdown>
                  : msg.content}
                {msg.displays?.map((d, j) =>
                  d.type === 'risk_heatmap' ? <RiskHeatmap key={j} display={d} /> : null
                )}
              </div>
            </div>
          ))}

          {isLoading && (
            <div className="forge-msg assistant">
              <div className="forge-label">AGENT</div>
              <div className="forge-bubble">
                <span className="forge-dots"><span /><span /><span /></span>
              </div>
            </div>
          )}

          <div ref={bottomRef} />
        </div>

        <div className="forge-input-row">
          <span className="forge-prompt-prefix">{'>'}</span>
          <textarea
            className="forge-textarea"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="enter command..."
            rows={1}
          />
          <button className="forge-send-btn" onClick={handleSend} disabled={isLoading}>
            {isLoading ? 'WAIT' : 'EXEC'}
          </button>
        </div>
      </div>

      {/* ── Dashboard links ── */}
      <nav className="forge-linkbar" aria-label="Cluster dashboards">
        {DASHBOARD_LINKS.map(l => (
          <a
            key={l.label}
            className="forge-link"
            href={l.href}
            target="_blank"
            rel="noopener noreferrer"
            title={l.title}
          >
            {l.icon}
            <span className="forge-link-label">{l.label}</span>
          </a>
        ))}
      </nav>

    </div>
  )
}
