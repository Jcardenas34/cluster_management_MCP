import 'katex/dist/katex.min.css'
import remarkGfm from 'remark-gfm'
import remarkMath from 'remark-math'
import rehypeKatex from 'rehype-katex'
import ReactMarkdown from 'react-markdown'


import { useState, useRef, useEffect } from 'react'

type Message = {
  role: 'user' | 'assistant'
  content: string
}

export default function ChatWindow() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // function handleSend() {
  //   if (!input.trim()) return

  //   const userMessage: Message = { role: 'user', content: input }
  //   setMessages(prev => [...prev, userMessage])
  //   setInput('')

  //   // Placeholder — replace with real API call later
  //   setTimeout(() => {
  //     const reply: Message = { role: 'assistant', content: 'Response coming soon...' }
  //     setMessages(prev => [...prev, reply])
  //   }, 500)
  // }

async function handleSend() {
  if (!input.trim()) return

  const userMessage: Message = { role: 'user', content: input }
  setMessages(prev => [...prev, userMessage])
  setInput('')

  try {
    const res = await fetch('http://localhost:8000/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: userMessage.content }),
    })
    const data = await res.json()
    const reply: Message = { role: 'assistant', content: data.reply }
    setMessages(prev => [...prev, reply])
  } catch {
    const error: Message = { role: 'assistant', content: 'Error: could not reach the server.' }
    setMessages(prev => [...prev, error])
  }
}


  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="chat-container">
      <div className="chat-header">Kitsune Cluster Manager</div>

      <div className="chat-messages">
        {messages.map((msg, i) => (
          <div key={i} className={`message ${msg.role}`}>
            <span className="bubble">{
              <span className="bubble">
                {msg.role === 'assistant'
                ? <ReactMarkdown remarkPlugins={[remarkMath, remarkGfm]} rehypePlugins={[rehypeKatex]}>
                  {msg.content}
                  </ReactMarkdown>
                : msg.content}
              </span>
            }
            </span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      <div className="chat-input-row">
        <textarea
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask the cluster manager..."
          rows={1}
        />
        <button onClick={handleSend}>Send</button>
      </div>
    </div>
  )
}
