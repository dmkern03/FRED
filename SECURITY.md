# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities
2. Email the maintainers directly with details of the vulnerability
3. Include steps to reproduce the issue if possible
4. Allow reasonable time for the issue to be addressed before public disclosure

## Security Best Practices for Users

### API Key Management

- **Never** commit your FRED API key to version control
- Use Databricks Secrets to store API keys securely:
  ```bash
  databricks secrets create-scope fred-api
  databricks secrets put-secret fred-api api-key --string-value "YOUR_KEY"
  ```
- Rotate your API keys periodically

### Databricks Security

- Use Unity Catalog for data governance
- Apply appropriate permissions to catalogs, schemas, and tables
- Use service principals for production deployments
- Enable audit logging in your Databricks workspace

### Environment Variables

- Use `.env` files for local development only
- Never commit `.env` files (they are in `.gitignore`)
- Use the provided `.env.example` as a template

## Dependencies

This project uses dependencies that are regularly updated. To check for known vulnerabilities:

```bash
pip install safety
safety check -r requirements.txt
```
