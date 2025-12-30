# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- MIT License for open source release
- Contributing guidelines (CONTRIBUTING.md)
- Code of Conduct (CODE_OF_CONDUCT.md)
- Security policy (SECURITY.md)
- GitHub issue and PR templates
- GitHub Actions CI workflow
- Pre-commit hooks configuration
- pyproject.toml for modern Python packaging

### Changed
- Improved README with badges and clearer structure
- Moved deprecated notebooks to archive folder

## [1.0.3] - 2025-01-15

### Changed
- Renamed "Rate" terminology to "Observation" for clarity
- Changed table types to Streaming Tables for Bronze and Silver layers
- Changed Gold layer to Materialized View for automatic refresh

### Added
- Production deployment documentation
- Environment-specific configurations (dev/prod)
- Azure environment settings

## [1.0.2] - 2025-01-10

### Added
- Delta Live Tables (DLT) Python API implementation
- Streaming Tables with Auto Loader for Bronze layer
- Data quality expectations using `@dlt.expect_all_or_drop`
- Databricks Asset Bundles configuration

### Changed
- Migrated from SQL-based approach to DLT Python API
- Updated pipeline to use cloud_files for incremental ingestion

## [1.0.1] - 2025-01-05

### Added
- Silver layer data quality constraints
- Primary/Foreign key constraint documentation
- Architecture documentation

### Fixed
- Data type conversions in Silver layer transformations

## [1.0.0] - 2025-01-01

### Added
- Initial release
- Bronze, Silver, and Gold layer setup
- FRED API integration for 15 rate series
- Daily API call notebook
- Basic project documentation
- Databricks secrets integration
