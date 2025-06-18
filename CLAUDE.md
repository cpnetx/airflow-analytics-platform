# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow 3.0.2 analytics platform for industrial process monitoring and reporting. Generates automated reports for manufacturing lines, tracking temperature controls and mechanical settings.

## Key Commands

### Docker Operations
```bash
# Build images with updated dependencies
docker compose build --pull --no-cache

# Start all services
docker compose up -d

# Restart services (e.g., after dependency updates)
docker compose down && docker compose up -d

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# Access container shell
docker compose exec airflow-scheduler bash

# Check installed packages
docker compose exec airflow-scheduler python -m pip list | grep package-name
```

### Development Workflow
```bash
# After updating requirements.txt
docker compose build --pull
docker compose down
docker compose up -d

# Verify Airflow version
docker compose exec airflow-scheduler airflow version

# Access Airflow UI
# URL: http://localhost:8080
# Default credentials: airflow/airflow
```

## Architecture

### Data Flow
1. **InfluxDB** → Time-series data for process parameters (temperatures, positions)
2. **ClickHouse** → Quality data and analytics (enhanced DAG only)
3. **Process Reporting DAGs** → Analyze changes, generate HTML reports
4. **Azure Blob Storage** → Archive reports
5. **Email Notifications** → Alert stakeholders

### Critical Files
- `dags/wpp/globals.yml` - Contains ALL credentials (InfluxDB tokens, Azure keys, OAuth)
- `dags/wpp/daily_report_config.yml` - Line configurations per site
- `requirements.txt` - Analytics dependencies (must rebuild images after changes)

### DAG Details

**process_change_report_dag.py** - Basic reporting
- Manual trigger only
- Simplified email (no OAuth)
- Basic change detection

**process_change_report_enhanced_dag.py** - Production reporting
- Scheduled: 6:02 AM/PM daily
- OAuth email authentication
- Quality reports from ClickHouse
- Alert status integration

### Manufacturing Sites
- **SCH**: Lines l6, l8, l9, l14, l15, l22
- **SV**: Lines l44, l46, l52, l53, l55

Each line has specific tag patterns for heat/non-heat controls defined in the config files.

## Working with Credentials

Credentials can be stored in two ways:
1. **globals.yml** file (current approach)
2. **Airflow Variables** (fallback when file unavailable)

Key variables if using Airflow Variables:
- `INFLUXDB_TOKEN_SCH`, `INFLUXDB_TOKEN_SV`
- `IQS_DB_CLICKHOUSE` (format: `clickhouse://user:pass@host:port/db`)
- `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`
- `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_SAS_TOKEN`

## Troubleshooting

### Common Issues
1. **Import errors after adding dependencies**: Rebuild images
2. **ClickHouse connection**: Uses native driver, not SQLAlchemy dialect
3. **Email sending**: Enhanced DAG requires OAuth setup
4. **Mock data**: Both DAGs generate mock data when real sources unavailable

### Data Analysis
- Reports compare current shift vs previous shift
- Thresholds: 10% change for heat controls, any change for non-heat
- Color coding: Red (heat controls), Yellow (non-heat controls)
- Shift times: Day (7 AM - 7 PM), Night (7 PM - 7 AM)

## Docker Service Architecture

All services use custom image with analytics libraries:
- `airflow-scheduler` - Core scheduling
- `airflow-worker` - Task execution (CeleryExecutor)
- `airflow-apiserver` - REST API (port 8080)
- `airflow-dag-processor` - DAG parsing
- `airflow-triggerer` - Async operations
- `postgres` - Metadata storage
- `redis` - Celery broker

## Git Workflow

### Creating Commits
Follow JIRA Smart Commit format:

`<JIRA_ISSUE_KEY> #time <time_spent> #comment <commit_summary>`

- Use current branch name as JIRA issue key (get with `git branch --show-current`)
- Specify time spent (e.g., `30m`, `1h`, `2h 15m`)
- Add all changed files first, then run diff to summarize changes
- Example: `CPNPROD-757 #time 50m #comment Refactor: Update OpenAI models and parameters.`

### Creating Pull Requests
1. Check diff between current branch and dev: `git diff dev...HEAD`
2. Use current branch name as PR title
3. Generate `pr_description.md` file with PR body content
4. Submit PR using: `gh pr create --base dev --title "<branch-name>" --body-file pr_description.md`
5. Wait for user confirmation before syncing and cleaning up local branch

## Commit Guidelines
- When commit do not add any claude specific messages and credit
- Do not credit claude in commit or PR message