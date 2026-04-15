import { useState } from 'react';

export default function Chart({ data, unit }) {
  const [hoveredPoint, setHoveredPoint] = useState(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });

  if (!data || data.length === 0) return null;

  const width = 800;
  const height = 400;
  const padding = 40;
  const chartWidth = width - 2 * padding;
  const chartHeight = height - 2 * padding;

  // Get min/max values
  const values = data.map(d => d.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueRange = maxValue - minValue || 1;

  // Scale functions
  const scaleX = (index) => padding + (index / (data.length - 1)) * chartWidth;
  const scaleY = (value) => height - padding - ((value - minValue) / valueRange) * chartHeight;

  // Handle mouse move - detect hovered point
  const handleMouseMove = (e) => {
    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const x = (e.clientX - rect.left) * (width / rect.width);
    const y = (e.clientY - rect.top) * (height / rect.height);
    
    setMousePos({ x, y });

    // Find nearest data point within tolerance
    let nearest = null;
    let minDist = 25;

    data.forEach((d, i) => {
      const px = scaleX(i);
      const py = scaleY(d.value);
      const dist = Math.sqrt((px - x) ** 2 + (py - y) ** 2);
      
      if (dist < minDist) {
        minDist = dist;
        nearest = i;
      }
    });

    setHoveredPoint(nearest);
  };

  const handleMouseLeave = () => {
    setHoveredPoint(null);
  };

  // Generate path for line
  const pathPoints = data.map((d, i) => `${scaleX(i)},${scaleY(d.value)}`).join(' ');
  const path = `M ${pathPoints}`;

  // Format time to Vietnam timezone (UTC+7) - Asia/Ho_Chi_Minh
  const formatTimeVN = (timestamp) => {
    try {
      const date = new Date(timestamp);
      return date.toLocaleString('vi-VN', { 
        timeZone: 'Asia/Ho_Chi_Minh',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
      });
    } catch {
      return '';
    }
  };

  // Generate grid lines and labels
  const gridLines = [];
  const yLabels = [];
  
  for (let i = 0; i <= 5; i++) {
    const y = (i / 5) * chartHeight;
    const value = maxValue - (i / 5) * valueRange;
    gridLines.push(
      <line
        key={`grid-${i}`}
        x1={padding}
        y1={height - padding - y}
        x2={width - padding}
        y2={height - padding - y}
        stroke="rgba(0, 212, 255, 0.1)"
        strokeWidth="1"
      />
    );
    yLabels.push(
      <text
        key={`label-${i}`}
        x={padding - 10}
        y={height - padding - y + 4}
        textAnchor="end"
        fontSize="12"
        fill="#888"
      >
        {value.toFixed(1)}
      </text>
    );
  }

  // X Axis time labels - show every Nth point
  const timeLabels = [];
  const step = Math.max(1, Math.floor(data.length / 5)); // Show ~5 time labels
  
  for (let i = 0; i < data.length; i += step) {
    const d = data[i];
    if (d.timestamp) {
      timeLabels.push(
        <text
          key={`time-${i}`}
          x={scaleX(i)}
          y={height - 15}
          textAnchor="middle"
          fontSize="11"
          fill="#00d4ff"
        >
          {formatTimeVN(d.timestamp)}
        </text>
      );
    }
  }

  return (
    <svg 
      width="100%" 
      height={height} 
      viewBox={`0 0 ${width} ${height}`} 
      style={{ minHeight: '300px', border: '1px solid rgba(0, 212, 255, 0.2)', borderRadius: '8px', cursor: 'crosshair' }}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      {/* Background */}
      <rect width={width} height={height} fill="rgba(26, 38, 64, 0.3)" />

      {/* Grid */}
      {gridLines}

      {/* Y Axis Labels */}
      {yLabels}

      {/* Y Axis */}
      <line x1={padding} y1={padding} x2={padding} y2={height - padding} stroke="#00d4ff" strokeWidth="2" />

      {/* X Axis */}
      <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} stroke="#00d4ff" strokeWidth="2" />

      {/* X Axis Time Labels (Vietnam Time) */}
      {timeLabels}

      {/* Chart Line */}
      <polyline
        points={pathPoints}
        fill="none"
        stroke="#00d4ff"
        strokeWidth="3"
        strokeLinecap="round"
        strokeLinejoin="round"
      />

      {/* Data Points */}
      {data.map((d, i) => (
        <circle
          key={`point-${i}`}
          cx={scaleX(i)}
          cy={scaleY(d.value)}
          r={hoveredPoint === i ? 7 : 4}
          fill={hoveredPoint === i ? '#FFD700' : '#00d4ff'}
          stroke={hoveredPoint === i ? '#FFA500' : 'rgba(15, 20, 25, 0.8)'}
          strokeWidth={hoveredPoint === i ? 3 : 2}
          style={{ transition: 'all 0.2s' }}
        />
      ))}

      {/* Tooltip */}
      {hoveredPoint !== null && data[hoveredPoint] && (
        <g>
          {/* Tooltip background */}
          <rect
            x={mousePos.x + 10}
            y={mousePos.y - 50}
            width="140"
            height="50"
            fill="rgba(0, 0, 0, 0.9)"
            stroke="#00d4ff"
            strokeWidth="1"
            rx="4"
          />
          {/* Tooltip text - Time */}
          <text
            x={mousePos.x + 20}
            y={mousePos.y - 32}
            fontSize="12"
            fill="#00d4ff"
            fontWeight="bold"
          >
            {formatTimeVN(data[hoveredPoint].timestamp)}
          </text>
          {/* Tooltip text - Value */}
          <text
            x={mousePos.x + 20}
            y={mousePos.y - 12}
            fontSize="14"
            fill="#FFD700"
            fontWeight="bold"
          >
            {data[hoveredPoint].value} {unit}
          </text>
        </g>
      )}

      {/* Y Axis Label */}
      <text
        x="20"
        y="20"
        fontSize="12"
        fill="#888"
        transform={`rotate(-90 20 20)`}
      >
        Value ({unit})
      </text>

      {/* X Axis Label */}
      <text x={width / 2} y={height - 10} textAnchor="middle" fontSize="12" fill="#888">
        Time
      </text>
    </svg>
  );
}
