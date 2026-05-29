import { useEffect, useMemo, useRef, useState } from 'react'
import { MessageCircle, Send, UserRound, Bot, Shield, LifeBuoy, Settings2, Plus, Pencil, Trash2, X } from 'lucide-react'
import api from '../api'
import { useAuth } from '../context/AuthContext'
import { formatVNDateTime } from '../utils/vnTime'

const STATUS_LABELS = {
  bot_active: 'Bot đang hỗ trợ',
  waiting_admin: 'Đang chờ admin',
  in_progress: 'Admin đang xử lý',
  closed: 'Đã đóng',
}

export default function SupportChat() {
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'

  const [conversations, setConversations] = useState([])
  const [selectedConversationId, setSelectedConversationId] = useState(null)
  const [messages, setMessages] = useState([])
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [thinkingConversationId, setThinkingConversationId] = useState(null)
  const [statusFilter, setStatusFilter] = useState('all')

  const [issueTemplates, setIssueTemplates] = useState([])
  const [showIssueManager, setShowIssueManager] = useState(false)
  const [showOtherIssueInput, setShowOtherIssueInput] = useState(false)
  const [otherIssueText, setOtherIssueText] = useState('')
  const [issueForm, setIssueForm] = useState({ title: '', description: '', sort_order: 0, is_active: true })
  const [editingIssueId, setEditingIssueId] = useState(null)

  const conversationsLoadingRef = useRef(false)
  const messagesLoadingRef = useRef(false)
  const templatesLoadingRef = useRef(false)

  const selectedConversation = useMemo(
    () => conversations.find((c) => c.id === selectedConversationId) || null,
    [conversations, selectedConversationId]
  )

  const otherIssueTemplate = useMemo(
    () => issueTemplates.find((i) => (i.title || '').toLowerCase() === 'other'),
    [issueTemplates]
  )

  const loadConversations = async () => {
    if (document.visibilityState === 'hidden') return
    if (conversationsLoadingRef.current) return
    try {
      conversationsLoadingRef.current = true
      const endpoint = isAdmin ? `/api/chat/admin/conversations?status=${statusFilter}` : '/api/chat/conversations'
      const res = await api.get(endpoint)
      const rows = res.data?.conversations || []
      setConversations(rows)
      const hasCurrentSelection = selectedConversationId && rows.some((c) => c.id === selectedConversationId)
      if (!hasCurrentSelection) {
        setSelectedConversationId(rows.length > 0 ? rows[0].id : null)
        if (rows.length === 0) {
          setMessages([])
        }
      }
    } catch (err) {
      console.error('Failed to load conversations:', err)
    } finally {
      conversationsLoadingRef.current = false
    }
  }

  const loadMessages = async (conversationId) => {
    if (!conversationId) return
    if (document.visibilityState === 'hidden') return
    // Keep optimistic user message visible while waiting bot response.
    if (loading) return
    if (messagesLoadingRef.current) return
    try {
      messagesLoadingRef.current = true
      const res = await api.get(`/api/chat/conversations/${conversationId}`)
      setMessages(res.data?.messages || [])
    } catch (err) {
      console.error('Failed to load messages:', err)
    } finally {
      messagesLoadingRef.current = false
    }
  }

  const loadIssueTemplates = async () => {
    if (document.visibilityState === 'hidden') return
    if (templatesLoadingRef.current) return
    try {
      templatesLoadingRef.current = true
      const endpoint = isAdmin ? '/api/chat/issue-templates?include_inactive=true' : '/api/chat/issue-templates'
      const res = await api.get(endpoint)
      setIssueTemplates(res.data?.items || [])
    } catch (err) {
      console.error('Failed to load issue templates:', err)
    } finally {
      templatesLoadingRef.current = false
    }
  }

  useEffect(() => {
    loadConversations()
    loadIssueTemplates()
    const interval = setInterval(() => {
      loadConversations()
    }, 8000)
    const templatesInterval = setInterval(() => {
      loadIssueTemplates()
    }, 60000)
    return () => {
      clearInterval(interval)
      clearInterval(templatesInterval)
    }
  }, [isAdmin, statusFilter])

  useEffect(() => {
    loadMessages(selectedConversationId)
    if (!selectedConversationId) return
    const interval = setInterval(() => loadMessages(selectedConversationId), 4000)
    return () => clearInterval(interval)
  }, [selectedConversationId])

  const sendUserMessage = async (contentOverride = null) => {
    const content = (contentOverride ?? message).trim()
    if (!content) return
    setMessage('')
    const fallbackConversationId = selectedConversationId
    const optimisticMessage = {
      id: `temp-${Date.now()}`,
      conversation_id: fallbackConversationId,
      sender_type: 'user',
      content,
      created_at: new Date().toISOString(),
    }
    try {
      setLoading(true)
      setMessages((prev) => [...prev, optimisticMessage])
      setThinkingConversationId(fallbackConversationId || -1)
      const res = await api.post('/api/chat/send', {
        conversation_id: selectedConversationId || null,
        message: content,
      })
      const cid = res.data?.conversation_id || selectedConversationId
      setShowOtherIssueInput(false)
      setOtherIssueText('')
      if (cid) setSelectedConversationId(cid)
      setThinkingConversationId(cid || null)
      await loadConversations()
      if (cid) await loadMessages(cid)
    } catch (err) {
      console.error('Failed to send user message:', err)
      setMessages((prev) => prev.filter((m) => m.id !== optimisticMessage.id))
    } finally {
      setThinkingConversationId(null)
      setLoading(false)
    }
  }

  const escalateToAdmin = async () => {
    try {
      setLoading(true)
      const res = await api.post('/api/chat/escalate', {
        conversation_id: selectedConversationId || null,
        reason: 'User requested human support from chat window.',
      })
      const cid = res.data?.conversation_id
      if (cid) setSelectedConversationId(cid)
      await loadConversations()
      if (cid) await loadMessages(cid)
    } catch (err) {
      console.error('Failed to escalate chat:', err)
    } finally {
      setLoading(false)
    }
  }

  const sendAdminReply = async () => {
    const content = message.trim()
    if (!content || !selectedConversationId) return
    try {
      setLoading(true)
      await api.post(`/api/chat/admin/conversations/${selectedConversationId}/reply`, { message: content })
      setMessage('')
      await loadConversations()
      await loadMessages(selectedConversationId)
    } catch (err) {
      console.error('Failed to send admin reply:', err)
    } finally {
      setLoading(false)
    }
  }

  const closeConversation = async () => {
    if (!selectedConversationId) return
    try {
      setLoading(true)
      await api.post(`/api/chat/admin/conversations/${selectedConversationId}/close`)
      await loadConversations()
      await loadMessages(selectedConversationId)
    } catch (err) {
      console.error('Failed to close conversation:', err)
    } finally {
      setLoading(false)
    }
  }

  const createNewConversation = async () => {
    if (isAdmin) return
    try {
      setLoading(true)
      const res = await api.post('/api/chat/conversations/new')
      const cid = res.data?.conversation_id
      await loadConversations()
      if (cid) {
        setSelectedConversationId(cid)
        await loadMessages(cid)
      }
    } catch (err) {
      console.error('Failed to create conversation:', err)
    } finally {
      setLoading(false)
    }
  }

  const deleteCurrentConversation = async () => {
    if (isAdmin || !selectedConversationId) return
    if (!confirm('Delete this conversation?')) return
    try {
      setLoading(true)
      await api.delete(`/api/chat/conversations/${selectedConversationId}`)
      setSelectedConversationId(null)
      setMessages([])
      await loadConversations()
    } catch (err) {
      console.error('Failed to delete conversation:', err)
    } finally {
      setLoading(false)
    }
  }

  const saveIssueTemplate = async () => {
    if (!issueForm.title.trim()) return
    try {
      setLoading(true)
      if (editingIssueId) {
        await api.patch(`/api/chat/admin/issue-templates/${editingIssueId}`, issueForm)
      } else {
        await api.post('/api/chat/admin/issue-templates', issueForm)
      }
      setIssueForm({ title: '', description: '', sort_order: 0, is_active: true })
      setEditingIssueId(null)
      await loadIssueTemplates()
    } catch (err) {
      console.error('Failed to save issue template:', err)
    } finally {
      setLoading(false)
    }
  }

  const editIssueTemplate = (item) => {
    setEditingIssueId(item.id)
    setIssueForm({
      title: item.title || '',
      description: item.description || '',
      sort_order: item.sort_order || 0,
      is_active: item.is_active,
    })
  }

  const removeIssueTemplate = async (id) => {
    try {
      setLoading(true)
      await api.delete(`/api/chat/admin/issue-templates/${id}`)
      await loadIssueTemplates()
    } catch (err) {
      console.error('Failed to delete issue template:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleQuickIssueClick = async (item) => {
    if (!item) return
    const isOther = (item.title || '').toLowerCase() === 'other'
    if (isOther) {
      setShowOtherIssueInput(true)
      return
    }
    const issueMessage = `[Common issue] ${item.title}${item.description ? ` - ${item.description}` : ''}`
    await sendUserMessage(issueMessage)
  }

  const handleSendOtherIssue = async () => {
    const detail = otherIssueText.trim()
    if (!detail) return
    await sendUserMessage(`[Common issue] Other - ${detail}`)
  }

  const handleSend = () => {
    if (isAdmin) {
      sendAdminReply()
      return
    }
    sendUserMessage()
  }

  return (
    <div className="p-6 h-full flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">{isAdmin ? 'Customer Messages' : 'Support Chat'}</h1>
          <p className="text-gray-400 mt-1">
            {isAdmin ? 'Admin can reply directly to customers' : 'Chat with bot, then escalate to admin if needed'}
          </p>
        </div>
        {isAdmin && (
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowIssueManager(true)}
              className="inline-flex items-center gap-2 bg-dark-900 border border-neon-cyan/40 rounded-lg px-3 py-2 text-neon-cyan hover:bg-neon-cyan/10 transition-all"
            >
              <Settings2 className="w-4 h-4" />
              Issue Templates
            </button>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
            >
              <option value="all">All escalated</option>
              <option value="waiting_admin">Waiting admin</option>
              <option value="in_progress">In progress</option>
            </select>
          </div>
        )}
        {!isAdmin && (
          <div className="flex items-center gap-2">
            <button
              onClick={createNewConversation}
              disabled={loading}
              className="px-3 py-2 rounded-lg bg-neon-cyan/15 text-neon-cyan border border-neon-cyan/40 hover:bg-neon-cyan/25 transition-all text-sm disabled:opacity-50"
            >
              New chat
            </button>
            <button
              onClick={deleteCurrentConversation}
              disabled={loading || !selectedConversationId}
              className="px-3 py-2 rounded-lg bg-red-500/15 text-red-300 border border-red-400/40 hover:bg-red-500/25 transition-all text-sm disabled:opacity-50"
            >
              Delete chat
            </button>
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 flex-1 min-h-0">
        <div className="card-border bg-dark-800 p-3 overflow-y-auto">
          <h3 className="text-white font-semibold mb-3 flex items-center gap-2">
            <MessageCircle className="w-4 h-4 text-neon-cyan" />
            Conversations
          </h3>
          <div className="space-y-2">
            {conversations.map((conv) => (
              <button
                key={conv.id}
                onClick={() => setSelectedConversationId(conv.id)}
                className={`w-full text-left p-3 rounded-lg border transition-all ${
                  selectedConversationId === conv.id
                    ? 'border-neon-cyan bg-neon-cyan/10'
                    : 'border-gray-700 bg-dark-900 hover:border-neon-cyan/40'
                }`}
              >
                <p className="text-white text-sm font-semibold">{STATUS_LABELS[conv.status] || conv.status}</p>
                {isAdmin && conv.user_name && (
                  <p className="text-neon-cyan text-xs mt-1">User: {conv.user_name} {conv.user_email ? `(${conv.user_email})` : ''}</p>
                )}
                <p className="text-gray-400 text-xs mt-1 line-clamp-2">{conv.last_message_preview || 'No message yet'}</p>
                <p className="text-gray-500 text-[11px] mt-1">{formatVNDateTime(conv.updated_at, true)} (GMT+7)</p>
              </button>
            ))}
            {conversations.length === 0 && <p className="text-gray-500 text-sm">No conversation yet.</p>}
          </div>
        </div>

        <div className="lg:col-span-2 card-border bg-dark-800 p-4 flex flex-col min-h-0">
          <div className="flex items-center justify-between pb-3 border-b border-gray-700">
            <div>
              <p className="text-white font-semibold">{selectedConversation ? 'Conversation' : 'Select a conversation'}</p>
              {selectedConversation && <p className="text-xs text-gray-400 mt-1">{STATUS_LABELS[selectedConversation.status] || selectedConversation.status}</p>}
              {isAdmin && selectedConversation?.user_name && (
                <p className="text-xs text-neon-cyan mt-1">
                  Chat with: {selectedConversation.user_name}{selectedConversation.user_email ? ` (${selectedConversation.user_email})` : ''}
                </p>
              )}
            </div>
            {!isAdmin && selectedConversation && (
              <button
                onClick={escalateToAdmin}
                disabled={loading || selectedConversation.status === 'waiting_admin' || selectedConversation.status === 'in_progress' || selectedConversation.status === 'closed'}
                className="px-3 py-2 rounded-lg bg-neon-yellow/15 text-neon-yellow border border-neon-yellow/40 hover:bg-neon-yellow/25 transition-all text-sm disabled:opacity-50"
              >
                Chat với admin
              </button>
            )}
            {isAdmin && selectedConversation && selectedConversation.status !== 'closed' && (
              <button
                onClick={closeConversation}
                disabled={loading}
                className="px-3 py-2 rounded-lg bg-red-500/20 text-red-300 border border-red-400/40 hover:bg-red-500/30 transition-all text-sm disabled:opacity-50"
              >
                Close chat
              </button>
            )}
          </div>

          <div className="flex-1 overflow-y-auto py-4 space-y-3">
            {messages.map((msg) => {
              const isSelf = (msg.sender_type === 'user' && !isAdmin) || (msg.sender_type === 'admin' && isAdmin)
              const icon = msg.sender_type === 'bot'
                ? <Bot className="w-4 h-4" />
                : msg.sender_type === 'admin'
                  ? <Shield className="w-4 h-4" />
                  : msg.sender_type === 'system'
                    ? <LifeBuoy className="w-4 h-4" />
                    : <UserRound className="w-4 h-4" />
              return (
                <div key={msg.id} className={`flex ${isSelf ? 'justify-end' : 'justify-start'}`}>
                  <div className={`max-w-[80%] px-4 py-3 rounded-xl border ${
                    msg.sender_type === 'system'
                      ? 'bg-amber-500/10 border-amber-500/30 text-amber-200'
                      : isSelf
                        ? 'bg-neon-cyan/20 border-neon-cyan/40 text-white'
                        : 'bg-dark-900 border-gray-700 text-gray-100'
                  }`}>
                    <div className="flex items-center gap-2 text-xs opacity-80 mb-1">
                      {icon}
                      <span className="capitalize">{msg.sender_type}</span>
                      <span>{formatVNDateTime(msg.created_at, true)} (GMT+7)</span>
                    </div>
                    <p className="whitespace-pre-wrap">{msg.content}</p>
                  </div>
                </div>
              )
            })}
            {!isAdmin && selectedConversation && thinkingConversationId === selectedConversation.id && (
              <div className="flex justify-start">
                <div className="max-w-[80%] px-4 py-3 rounded-xl border bg-dark-900 border-gray-700 text-gray-100">
                  <div className="flex items-center gap-2 text-xs opacity-80 mb-1">
                    <Bot className="w-4 h-4" />
                    <span>bot</span>
                  </div>
                  <p className="text-base text-gray-200 animate-pulse">Bot đang suy luận...</p>
                </div>
              </div>
            )}
            {selectedConversation && messages.length === 0 && <p className="text-gray-500 text-sm">No message yet.</p>}
          </div>

          <div className="pt-3 border-t border-gray-700 flex gap-2">
            <input
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  handleSend()
                }
              }}
              placeholder={isAdmin ? 'Reply to customer...' : 'Message bot or admin...'}
              className="flex-1 bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
              disabled={!isAdmin && selectedConversation?.status === 'closed'}
            />
            <button
              onClick={handleSend}
              disabled={loading || !message.trim() || (!isAdmin && selectedConversation?.status === 'closed')}
              className="px-4 py-2 rounded-lg bg-neon-cyan/20 border border-neon-cyan/50 text-neon-cyan hover:bg-neon-cyan/30 transition-all disabled:opacity-50"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
          {!isAdmin && selectedConversation?.status === 'closed' && (
            <p className="mt-2 text-xs text-amber-300">
              Cuộc hội thoại này đã đóng. Hãy bấm <strong>New chat</strong> để tiếp tục trao đổi.
            </p>
          )}

          {!isAdmin && selectedConversation && (
            <div className="mt-3 border-t border-gray-700 pt-3">
              <p className="text-xs text-gray-400 mb-2">Common issues</p>
              <div className="flex gap-2 overflow-x-auto pb-1">
                {issueTemplates.filter((item) => item.is_active !== false).map((item) => (
                  <button
                    key={item.id}
                    onClick={() => handleQuickIssueClick(item)}
                    className="shrink-0 px-3 py-2 rounded-full bg-neon-cyan/15 border border-neon-cyan/40 text-neon-cyan text-sm hover:bg-neon-cyan/25 transition-all"
                  >
                    {item.title}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {showOtherIssueInput && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
          <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl w-full max-w-lg p-5">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xl font-bold text-white">Other issue</h3>
              <button onClick={() => setShowOtherIssueInput(false)} className="text-gray-400 hover:text-white">
                <X className="w-5 h-5" />
              </button>
            </div>
            <textarea
              value={otherIssueText}
              onChange={(e) => setOtherIssueText(e.target.value)}
              rows={4}
              placeholder="Describe your issue..."
              className="w-full bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-neon-cyan outline-none"
            />
            <div className="mt-3 flex gap-2 justify-end">
              <button onClick={() => setShowOtherIssueInput(false)} className="px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600">Cancel</button>
              <button onClick={handleSendOtherIssue} className="px-3 py-2 bg-neon-cyan/20 text-neon-cyan border border-neon-cyan/40 rounded-lg hover:bg-neon-cyan/30">Send</button>
            </div>
          </div>
        </div>
      )}

      {isAdmin && showIssueManager && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
          <div className="bg-dark-800 border border-neon-cyan/30 rounded-xl w-full max-w-4xl p-5">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-2xl font-bold text-white">Manage Issue Templates</h3>
              <button onClick={() => setShowIssueManager(false)} className="text-gray-400 hover:text-white">
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2 mb-2">
              <input
                value={issueForm.title}
                onChange={(e) => setIssueForm((prev) => ({ ...prev, title: e.target.value }))}
                placeholder="Issue title"
                className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-purple-300 outline-none"
              />
              <input
                value={issueForm.description}
                onChange={(e) => setIssueForm((prev) => ({ ...prev, description: e.target.value }))}
                placeholder="Issue description"
                className="bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-purple-300 outline-none"
              />
            </div>
            <div className="flex gap-2 mb-3">
              <input
                type="number"
                value={issueForm.sort_order}
                onChange={(e) => setIssueForm((prev) => ({ ...prev, sort_order: Number(e.target.value) || 0 }))}
                className="w-32 bg-dark-900 border border-gray-700 rounded-lg px-3 py-2 text-white focus:border-purple-300 outline-none"
              />
              <label className="text-sm text-gray-200 flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={issueForm.is_active}
                  onChange={(e) => setIssueForm((prev) => ({ ...prev, is_active: e.target.checked }))}
                />
                Active
              </label>
              <button
                onClick={saveIssueTemplate}
                disabled={loading || !issueForm.title.trim()}
                className="px-3 py-2 rounded-lg bg-purple-500/20 border border-purple-400/50 text-purple-200 hover:bg-purple-500/30 transition-all disabled:opacity-50"
              >
                <Plus className="w-4 h-4 inline mr-1" />
                {editingIssueId ? 'Update' : 'Add'}
              </button>
            </div>
            <div className="space-y-2 max-h-[45vh] overflow-y-auto">
              {issueTemplates.map((item) => (
                <div key={item.id} className="flex items-center justify-between text-sm border border-gray-700 rounded px-3 py-2">
                  <div>
                    <p className="text-gray-100">{item.sort_order}. {item.title} {!item.is_active ? '(inactive)' : ''}</p>
                    {item.description ? <p className="text-gray-400 text-xs">{item.description}</p> : null}
                  </div>
                  <div className="flex gap-2">
                    <button onClick={() => editIssueTemplate(item)} className="text-blue-300 hover:text-blue-200"><Pencil className="w-4 h-4" /></button>
                    <button onClick={() => removeIssueTemplate(item.id)} className="text-red-300 hover:text-red-200"><Trash2 className="w-4 h-4" /></button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
