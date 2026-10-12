import './canopy.css'
import 'katex/dist/katex.min.css'
import remarkGfm from 'remark-gfm'
import remarkMath from 'remark-math'
import rehypeKatex from 'rehype-katex'
import ReactMarkdown from 'react-markdown'
import { useState, useRef, useEffect } from 'react'

type Message = {
  role: 'user' | 'assistant'
  content: string
  time: string
}

function getTime() {
  return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

export default function ChatWindowCanopy() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const pendingRef = useRef(false)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  async function handleSend() {
    if (!input.trim() || pendingRef.current) return
    pendingRef.current = true

    const userMessage: Message = { role: 'user', content: input, time: getTime() }
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      const res = await fetch('/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: userMessage.content }),
      })
      if (res.status === 409) {
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: 'Still working on the previous request — please wait.',
          time: getTime(),
        }])
        return
      }
      const data = await res.json()
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply, time: getTime() }])
    } catch {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Error: could not reach the server.',
        time: getTime(),
      }])
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

  return (
    <div className="canopy-container">
      <div className="canopy-header">
        <div className="canopy-header-left">
          <div className="canopy-header-title">Kitsune Cluster Manager</div>
          <div className="canopy-header-sub">Operations Center</div>
        </div>
        <div className="canopy-nodes">
          <div className="canopy-node"><div className="canopy-node-dot" />master</div>
          <div className="canopy-node"><div className="canopy-node-dot" />w-01</div>
          <div className="canopy-node"><div className="canopy-node-dot" />w-02</div>
          <div className="canopy-node"><div className="canopy-node-dot" />kafka</div>
          <div className="canopy-node"><div className="canopy-node-dot" />jetson</div>
        </div>
      </div>

      <div className="canopy-messages">
        {messages.length === 0 && (
          <div className="canopy-empty">
            <div className="canopy-empty-icon">⬡</div>
            <div className="canopy-empty-title">Cluster Manager Ready</div>
            <div className="canopy-empty-sub">Ask about cluster health, jobs, or node status</div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`canopy-msg ${msg.role}`}>
            <div className="canopy-avatar">
              {msg.role === 'user' ? 'OP' : '⬡'}
            </div>
            <div className="canopy-msg-body">
              <div className="canopy-timestamp">{msg.time}</div>
              <div className="canopy-bubble">
                {msg.role === 'assistant'
                  ? <ReactMarkdown remarkPlugins={[remarkMath, remarkGfm]} rehypePlugins={[rehypeKatex]}>
                      {msg.content}
                    </ReactMarkdown>
                  : msg.content}
              </div>
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="canopy-msg assistant">
            <div className="canopy-avatar">⬡</div>
            <div className="canopy-msg-body">
              <div className="canopy-bubble canopy-thinking">
                <span className="canopy-dots">
                  <span /><span /><span />
                </span>
              </div>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      <div className="canopy-input-area">
        <div className="canopy-input-inner">
          <textarea
            className="canopy-textarea"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Message the cluster manager..."
            rows={1}
          />
          <button className="canopy-send-btn" onClick={handleSend} disabled={isLoading}>
            {isLoading
              ? <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                  <circle cx="12" cy="12" r="10" />
                  <path d="M12 6v6l4 2" />
                </svg>
              : <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                  <line x1="22" y1="2" x2="11" y2="13" />
                  <polygon points="22 2 15 22 11 13 2 9 22 2" />
                </svg>
            }
          </button>
        </div>
      </div>
    </div>
  )
}
