# Contributing to FRED Data Pipeline

Thank you for your interest in contributing to the FRED Data Pipeline project! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)

## Code of Conduct

This project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/FRED.git
   cd FRED
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/dmkern03/FRED.git
   ```

## Development Setup

### Prerequisites

- Python 3.9 or higher
- Databricks CLI (for deployment testing)
- A FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html))

### Local Environment

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -e ".[dev]"
   ```

3. Set up pre-commit hooks:
   ```bash
   pre-commit install
   ```

4. Copy the environment template:
   ```bash
   cp .env.example .env
   # Edit .env with your FRED API key
   ```

### Databricks Environment

For testing DLT pipelines, you'll need:

1. A Databricks workspace with Unity Catalog enabled
2. Appropriate permissions to create catalogs, schemas, and pipelines
3. Configure Databricks CLI:
   ```bash
   databricks configure --profile dev
   ```

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues. When creating a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs actual behavior
- Databricks runtime version (if applicable)
- Any relevant logs or error messages

### Suggesting Features

Feature suggestions are welcome! Please include:

- A clear description of the feature
- The motivation/use case
- Any implementation ideas you have

### Code Contributions

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following our [coding standards](#coding-standards)

3. Write or update tests as needed

4. Commit your changes:
   ```bash
   git commit -m "Add: brief description of changes"
   ```

5. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

6. Open a Pull Request

## Pull Request Process

1. Ensure all tests pass locally
2. Update documentation if needed
3. Fill out the PR template completely
4. Link any related issues
5. Request review from maintainers
6. Address any feedback

### Commit Message Format

Use clear, descriptive commit messages:

```
<type>: <description>

[optional body]

[optional footer]
```

Types:
- `Add`: New feature
- `Fix`: Bug fix
- `Update`: Enhancement to existing feature
- `Refactor`: Code refactoring
- `Docs`: Documentation changes
- `Test`: Adding or updating tests
- `Chore`: Maintenance tasks

## Coding Standards

### Python

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints where practical
- Write docstrings for public functions and classes
- Maximum line length: 100 characters

### Databricks Notebooks

- Include a header comment explaining the notebook's purpose
- Use markdown cells for documentation
- Keep cells focused and well-organized
- Include `# COMMAND ----------` separators

### SQL

- Use UPPERCASE for SQL keywords
- Use snake_case for table and column names
- Include comments for complex queries

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_fred_pipeline.py
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Use descriptive test function names
- Include both positive and negative test cases

## Documentation

- Update the README for user-facing changes
- Update docstrings for code changes
- Add to `docs/` for architectural changes
- Keep documentation current with code

### Documentation Structure

- `README.md` - Project overview and quick start
- `docs/architecture.md` - System design
- `docs/data_dictionary.md` - Schema documentation
- `docs/dlt_pipeline_setup.md` - DLT setup guide
- `docs/production_deployment.md` - Production deployment

## Questions?

Feel free to open an issue for questions or reach out to the maintainers.

Thank you for contributing!
