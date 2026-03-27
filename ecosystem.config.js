module.exports = {
  apps: [
    {
      name: 'translate-stat-app',
      cwd: '/Users/zhiying8710/wk/translate-stat-app',
      script: './src/server.js',
      interpreter: 'node',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_memory_restart: '300M',
      env: {
        NODE_ENV: 'production',
        HOST: '0.0.0.0',
        PORT: 3000,
        APP_TIMEZONE: 'Asia/Shanghai',
        RETENTION_DAYS: 30
      }
    }
  ]
};
