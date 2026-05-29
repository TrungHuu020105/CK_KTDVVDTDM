import { useState, useEffect } from 'react'
import api from '../api'
import SimpleGauge from './SimpleGauge'
import { formatVNDateTime, formatVNTime } from '../utils/vnTime'

export default function ClientMonitor() {
  const [clients, setClients] = useState([])
  const [selectedClient, setSelectedClient] = useState(null)
  const [clientData, setClientData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // Lấy danh sách clients
  useEffect(() => {
    const fetchClients = async () => {
      try {
        setLoading(true)
        const response = await api.get('/api/status')
        const clientList = response.data.clients || []
        setClients(clientList)
        
        if (clientList.length > 0) {
          setSelectedClient(clientList[0].client_id)
          setClientData(clientList[0])
        }
        setError(null)
      } catch (err) {
        setError('Không thể lấy danh sách clients')
        console.error('Error fetching clients:', err)
      } finally {
        setLoading(false)
      }
    }

    fetchClients()
    // Refresh danh sách clients mỗi 2 giây
    const interval = setInterval(fetchClients, 2000)
    return () => clearInterval(interval)
  }, [])

  // Cập nhật dữ liệu khi client được chọn thay đổi
  useEffect(() => {
    if (!selectedClient) return

    const fetchClientData = async () => {
      try {
        const response = await api.get(`/api/status/${selectedClient}`)
        setClientData(response.data)
        setError(null)
      } catch (err) {
        setError(`Không thể lấy dữ liệu client: ${selectedClient}`)
        console.error('Error fetching client data:', err)
      }
    }

    fetchClientData()
    // Refresh dữ liệu mỗi 1 giây
    const interval = setInterval(fetchClientData, 1000)
    return () => clearInterval(interval)
  }, [selectedClient])

  return (
    <div className="p-6 bg-gradient-to-br from-gray-900 to-gray-800 min-h-screen text-white">
      <h1 className="text-4xl font-bold mb-8 text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-cyan-400">
        💻 Client Monitoring
      </h1>

      {error && (
        <div className="bg-red-500/20 border border-red-500 text-red-200 px-4 py-3 rounded-lg mb-6">
          {error}
        </div>
      )}

      {/* Select Client */}
      <div className="mb-8">
        <label className="block text-sm font-semibold mb-3 text-gray-300">
          Chọn Client:
        </label>
        <div className="flex gap-4 items-center">
          <select
            value={selectedClient || ''}
            onChange={(e) => setSelectedClient(e.target.value)}
            className="flex-1 px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-400 focus:ring-opacity-50"
          >
            <option value="">--Chọn một client--</option>
            {clients.map((client) => (
              <option key={client.client_id} value={client.client_id}>
                {client.client_id} ({client.status})
              </option>
            ))}
          </select>
          <span className="text-gray-400">
            Tổng: {clients.length} clients
          </span>
        </div>
      </div>

      {/* Client Data */}
      {clientData ? (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-8 shadow-2xl">
          <div className="mb-8">
            <h2 className="text-2xl font-bold mb-4 text-cyan-400">
              📊 {selectedClient}
            </h2>
            <div className="grid grid-cols-2 gap-6 text-gray-300">
              <div>
                <span className="text-gray-400">Status:</span>
                <p className="text-lg font-semibold mt-1">
                  <span
                    className={`inline-block px-3 py-1 rounded-full text-sm ${
                      clientData.status === 'connected'
                        ? 'bg-green-500/20 text-green-400'
                        : 'bg-red-500/20 text-red-400'
                    }`}
                  >
                    {clientData.status === 'connected' ? '🟢 Connected' : '🔴 Disconnected'}
                  </span>
                </p>
              </div>
              <div>
                <span className="text-gray-400">Kết nối lúc:</span>
                <p className="text-lg font-semibold mt-1">
                  {formatVNDateTime(clientData.connected_at)}
                </p>
              </div>
              <div>
                <span className="text-gray-400">Cập nhật lần cuối:</span>
                <p className="text-lg font-semibold mt-1">
                  {formatVNDateTime(clientData.last_update)}
                </p>
              </div>
            </div>
          </div>

          {/* Metrics */}
          {clientData.metrics && (
            <div className="mt-8">
              <h3 className="text-xl font-bold mb-6 text-cyan-400">Metrics Hiện Tại</h3>
              <div className="grid grid-cols-2 gap-8">
                {/* CPU */}
                <div className="flex flex-col items-center">
                  <SimpleGauge
                    value={clientData.metrics.cpu}
                    label="CPU Usage"
                    color="from-blue-500 to-blue-600"
                  />
                  <div className="text-center mt-4">
                    <p className="text-3xl font-bold text-blue-400">
                      {clientData.metrics.cpu.toFixed(1)}%
                    </p>
                    <p className="text-gray-400 text-sm">CPU</p>
                  </div>
                </div>

                {/* RAM */}
                <div className="flex flex-col items-center">
                  <SimpleGauge
                    value={clientData.metrics.ram}
                    label="RAM Usage"
                    color="from-purple-500 to-purple-600"
                  />
                  <div className="text-center mt-4">
                    <p className="text-3xl font-bold text-purple-400">
                      {clientData.metrics.ram.toFixed(1)}%
                    </p>
                    <p className="text-gray-400 text-sm">RAM</p>
                  </div>
                </div>
              </div>

              {/* Timestamp */}
              <div className="mt-6 text-center text-gray-400">
                <p className="text-sm">
                  Cập nhật: {formatVNDateTime(clientData.metrics.timestamp)}
                </p>
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-8 text-center">
          {loading ? (
            <p className="text-gray-400">🔄 Đang tải dữ liệu...</p>
          ) : (
            <p className="text-gray-400">Vui lòng chọn hoặc chờ clients kết nối...</p>
          )}
        </div>
      )}

      {/* Client List Info */}
      <div className="mt-8 bg-gray-800 rounded-xl border border-gray-700 p-6">
        <h3 className="text-lg font-bold mb-4 text-cyan-400">📋 Danh Sách Clients</h3>
        {clients.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-700">
                <tr>
                  <th className="px-4 py-2 text-left">Client ID</th>
                  <th className="px-4 py-2 text-left">Status</th>
                  <th className="px-4 py-2 text-right">CPU</th>
                  <th className="px-4 py-2 text-right">RAM</th>
                  <th className="px-4 py-2 text-left">Last Update</th>
                </tr>
              </thead>
              <tbody>
                {clients.map((client) => (
                  <tr
                    key={client.client_id}
                    onClick={() => setSelectedClient(client.client_id)}
                    className={`cursor-pointer border-t border-gray-700 ${
                      selectedClient === client.client_id
                        ? 'bg-blue-500/20'
                        : 'hover:bg-gray-700/50'
                    }`}
                  >
                    <td className="px-4 py-3">{client.client_id}</td>
                    <td className="px-4 py-3">
                      <span
                        className={`inline-block px-2 py-1 rounded text-xs ${
                          client.status === 'connected'
                            ? 'bg-green-500/20 text-green-400'
                            : 'bg-red-500/20 text-red-400'
                        }`}
                      >
                        {client.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right">
                      {client.metrics ? `${client.metrics.cpu.toFixed(1)}%` : 'N/A'}
                    </td>
                    <td className="px-4 py-3 text-right">
                      {client.metrics ? `${client.metrics.ram.toFixed(1)}%` : 'N/A'}
                    </td>
                    <td className="px-4 py-3 text-right text-gray-400 text-xs">
                      {formatVNTime(client.last_update)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-gray-400">Chưa có clients kết nối</p>
        )}
      </div>
    </div>
  )
}
