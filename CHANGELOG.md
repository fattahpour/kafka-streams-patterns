# Changelog

## [Unreleased] - 2025-11-21

### Fixed
- Downgraded Java version from 25 to 17 to ensure compatibility with GitHub Actions.
- Resolved Docker Compose port conflict by moving Kafka external port to 9093.
- Fixed `ksqldb-server` image version (downgraded to 0.29.0).
- Added `LogAndContinueExceptionHandler` to `enrichment-ktable` to prevent stream thread death on deserialization errors.

### Documentation
- Added standard GitHub community files:
    - Code of Conduct (`.github/CODE_OF_CONDUCT.md`)
    - Contributing Guide (`.github/CONTRIBUTING.md`)
    - Issue Templates (`.github/ISSUE_TEMPLATE/`)
    - Pull Request Template (`.github/PULL_REQUEST_TEMPLATE.md`)
    - Security Policy (`.github/SECURITY.md`)
- Updated `README.md` with correct Java version and new Docker port instructions.

## Added

- Wall-clock timers pattern for cron-like scheduling with punctuators.
- Event splitter pattern that creates lineage-aware child events and DLQ routing.
- Event collaboration pattern that tolerates out-of-order heterogeneous inputs.
- CQRS projections pattern for versioned state transitions and DLQ handling.
- Saga orchestration pattern coordinating order workflows with compensations.
- Geo replication notes with MirrorMaker 2 configuration samples.
- Pipeline strangler router to duplicate or divert traffic via feature flags.
- Content filter pattern to drop banned or oversized payloads early.
- Projection table TTL pattern with versioned upserts and expiry streams.
- GitHub Actions workflow and CI Maven profile.
- Repository README table summarising all modules.
