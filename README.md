# Airflow Analytics Platform

Apache Airflow 3.0.2 platform with integrated analytics libraries for process reporting and data analysis.

## Features

- **Apache Airflow 3.0.2** - Latest stable version with modern features
- **CeleryExecutor** - Distributed task execution with Redis backend
- **Analytics Stack** - Pre-installed data processing libraries:
  - `influxdb-client` - Time series data storage and retrieval
  - `clickhouse-driver` - Analytics database connectivity
  - `scipy`, `pandas`, `numpy` - Scientific computing and data analysis
  - `azure-storage-blob` - Cloud storage integration
- **Process Reporting** - Automated DAGs for generating process change reports

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/cpnetx/airflow-analytics-platform.git
cd airflow-analytics-platform
```

2. Build and start the services:
```bash
docker compose build --pull
docker compose up -d
```

3. Access Airflow UI at http://localhost:8080
   - Default username: `airflow`
   - Default password: `airflow`

## Project Structure

```
.
├── dags/                    # Airflow DAG definitions
│   ├── process_change_report_dag.py
│   └── process_change_report_enhanced_dag.py
├── config/                  # Airflow configuration
├── logs/                    # Airflow logs (gitignored)
├── plugins/                 # Custom Airflow plugins
├── docker-compose.yaml      # Docker Compose orchestration
├── Dockerfile              # Custom Airflow image
└── requirements.txt        # Python dependencies
```

## Configuration

### Environment Variables

Key configuration can be set via environment variables or `.env` file:

- `AIRFLOW_UID` - User ID for Airflow containers (default: 50000)
- `AIRFLOW_PROJ_DIR` - Project directory path (default: .)
- `_AIRFLOW_WWW_USER_USERNAME` - Admin username (default: airflow)
- `_AIRFLOW_WWW_USER_PASSWORD` - Admin password (default: airflow)

### Custom Dependencies

To add more Python packages, update `requirements.txt` and rebuild:

```bash
docker compose build --no-cache
docker compose up -d
```

## Development

### Adding New DAGs

Place new DAG files in the `dags/` directory. They will be automatically picked up by Airflow.

### Updating Dependencies

1. Edit `requirements.txt`
2. Rebuild the Docker image:
   ```bash
   docker compose build --pull
   docker compose down
   docker compose up -d
   ```

## Maintenance

### View Logs

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

### Access Container Shell

```bash
docker compose exec airflow-scheduler bash
```

### Database Backup

The PostgreSQL database contains all DAG metadata and history:

```bash
docker compose exec postgres pg_dump -U airflow airflow > backup.sql
```

## Security Notes

- Change default passwords before production use
- Store sensitive configuration in environment variables
- Use secrets management for credentials
- Enable authentication for production deployments

## License

This project is configured for internal use. Please refer to your organization's licensing policies.