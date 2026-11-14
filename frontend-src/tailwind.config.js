module.exports = {
  darkMode: 'class',
  content: ['../frontend/index.html'],
  theme: { extend: {
    colors: {
      ink: { 50:'#f7f7f8',100:'#eeeff1',200:'#d9dce0',300:'#b6bcc4',400:'#8b939e',500:'#646d79',600:'#475061',700:'#343c4c',800:'#222936',900:'#161b24',950:'#0c0f16' },
      acc: { 50:'#eef5ff',100:'#dae8ff',200:'#bcd6ff',300:'#8fbdff',400:'#5c9bff',500:'#3a7bf6',600:'#2359d8',700:'#1d46ad',800:'#1d3c8a',900:'#1d356e' },
      ok:'#1f9d55', warn:'#d97706', err:'#dc2626', info:'#3a7bf6'
    },
    fontFamily: { sans:['"Inter"','system-ui','Segoe UI','PingFang SC','Microsoft YaHei','sans-serif'], mono:['"JetBrains Mono"','ui-monospace','Consolas','monospace'] }
  }},
  plugins: []
};
