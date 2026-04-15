/** @type {import('tailwindcss').Config} */
export default {
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
        }
      }
    },
  },
  plugins: [],
}
