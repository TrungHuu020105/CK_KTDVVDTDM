import { useEffect, useState } from 'react'
import { Plus, Pencil, Trash2 } from 'lucide-react'
import api from '../api'

const EMPTY_ISSUE_FORM = { title: '', description: '', sort_order: 0, is_active: true }

export default function AdminIssueTemplates() {
  const [items, setItems] = useState([])
  const [issueForm, setIssueForm] = useState(EMPTY_ISSUE_FORM)
  const [editingIssueId, setEditingIssueId] = useState(null)
  const [loading, setLoading] = useState(false)

  const loadItems = async () => {
    try {
      const res = await api.get('/api/chat/issue-templates?include_inactive=true')
      setItems(res.data?.items || [])
    } catch (err) {
      console.error('Failed to load issue templates:', err)
    }
  }

  useEffect(() => {
    loadItems()
  }, [])

  const saveIssueTemplate = async () => {
    if (!issueForm.title.trim()) return
    try {
      setLoading(true)
      if (editingIssueId) {
        await api.patch(`/api/chat/admin/issue-templates/${editingIssueId}`, issueForm)
      } else {
        await api.post('/api/chat/admin/issue-templates', issueForm)
      }
      setIssueForm(EMPTY_ISSUE_FORM)
      setEditingIssueId(null)
      await loadItems()
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
      await loadItems()
    } catch (err) {
      console.error('Failed to delete issue template:', err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-3xl font-bold text-white">Quan Ly Van De Thuong Gap</h1>
        <p className="text-gray-400 mt-1">Admin quan ly danh sach van de de user chon nhanh khi chat</p>
      </div>

      <div className="card-border bg-dark-800 p-4">
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
          {editingIssueId && (
            <button
              onClick={() => {
                setEditingIssueId(null)
                setIssueForm(EMPTY_ISSUE_FORM)
              }}
              className="px-3 py-2 rounded-lg bg-gray-700 text-white hover:bg-gray-600 transition-all"
            >
              Cancel
            </button>
          )}
        </div>

        <div className="space-y-2">
          {items.map((item) => (
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
  )
}
