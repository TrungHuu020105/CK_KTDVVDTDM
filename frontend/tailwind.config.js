/** @type {import('tailwindcss').Config} */
export default {
  safelist: [
    'border-neon-cyan/30', 'border-neon-cyan/40', 'border-neon-cyan/60', 'bg-neon-cyan/20', 'text-neon-cyan', 'hover:shadow-neon-cyan/20',
    'border-neon-purple/30', 'border-neon-purple/40', 'border-neon-purple/60', 'bg-neon-purple/20', 'text-neon-purple', 'hover:shadow-neon-purple/20',
    'border-neon-green/30', 'border-neon-green/40', 'border-neon-green/60', 'bg-neon-green/20', 'text-neon-green', 'hover:shadow-neon-green/20',
    'border-neon-yellow/30', 'border-neon-yellow/40', 'border-neon-yellow/60', 'bg-neon-yellow/20', 'text-neon-yellow', 'hover:shadow-neon-yellow/20',
    'border-neon-orange/30', 'border-neon-orange/40', 'border-neon-orange/60', 'bg-neon-orange/20', 'text-neon-orange', 'hover:shadow-neon-orange/20',
    'border-neon-red/30', 'border-neon-red/40', 'border-neon-red/60', 'bg-neon-red/20', 'text-neon-red', 'hover:shadow-neon-red/20',
  ],
  content: [
    "./index.html",
    "./src/**/*.{js,jsx}",
  ],
  theme: {
    extend: {
      colors: {
        dark: {
          900: '#0a0e27',
          800: '#1a1f3a',
          700: '#2a2f4a',
          600: '#3a3f5a',
        },
        neon: {
          cyan: '#00f0ff',
          purple: '#c400ff',
          green: '#00ff88',
          yellow: '#ffaa00',
          orange: '#ff8a00',
          red: '#ff3b3b',
        }
      }
    },
  },
  plugins: [],
}
