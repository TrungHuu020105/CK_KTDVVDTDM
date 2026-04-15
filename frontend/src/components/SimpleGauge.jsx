export default function SimpleGauge({ value = 0, label = '', color = 'from-blue-500 to-blue-600' }) {
  const percentage = Math.min(Math.max(value, 0), 100)
  
  return (
    <div className="flex flex-col items-center">
      <div className="relative w-32 h-32 mb-4">
        {/* Outer circle background */}
        <svg className="w-full h-full transform -rotate-90" viewBox="0 0 120 120">
          {/* Background circle */}
          <circle
            cx="60"
            cy="60"
            r="50"
            fill="none"
            stroke="rgba(75, 85, 99, 0.3)"
            strokeWidth="8"
          />
          {/* Progress circle */}
          <circle
            cx="60"
            cy="60"
            r="50"
            fill="none"
            stroke="url(#gaugeGradient)"
            strokeWidth="8"
            strokeDasharray={`${(percentage / 100) * 314} 314`}
            strokeLinecap="round"
            className="transition-all duration-300"
          />
          <defs>
            <linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="currentColor" />
              <stop offset="100%" stopColor="currentColor" />
            </linearGradient>
          </defs>
        </svg>
        
        {/* Center percentage */}
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="text-center">
            <div className={`text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r ${color}`}>
              {percentage.toFixed(0)}%
            </div>
          </div>
        </div>
      </div>
      
      {label && <p className="text-gray-400 text-sm mt-2">{label}</p>}
    </div>
  )
}
