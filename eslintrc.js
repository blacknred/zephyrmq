module.exports = {
  root: true,
  env: {
    es2022: true,
    node: true,
    jest: true
  },
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  ignores: ['**/node_modules/**', '**/dist/**'],
  rules: {
    'no-console': 'off' // ['warn', { allow: ['warn', 'error'] }]
  }
};