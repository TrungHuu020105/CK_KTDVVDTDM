export default function Chart({ data, unit }) {
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

  // Generate path for line
  const pathPoints = data.map((d, i) => `${scaleX(i)},${scaleY(d.value)}`).join(' ');
  const path = `M ${pathPoints}`;

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

  return (
    <svg width="100%" height={height} viewBox={`0 0 ${width} ${height}`} style={{ minHeight: '300px', border: '1px solid rgba(0, 212, 255, 0.2)', borderRadius: '8px' }}>
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
          r="4"
          fill="#00d4ff"
          stroke="rgba(15, 20, 25, 0.8)"
          strokeWidth="2"
        />
      ))}

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
