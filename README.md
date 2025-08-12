# insight-engine

This service ingests AUV telemetry, stores it in PostgreSQL+PostGIS, and generates alerts for environmental thresholds, zone violations, and dead AUVs.

`mock-telemetry` for mocking AUVs telemetry data. Run `python mock-telemetry/main.py` using the `insight-engine AUV` virtual environment to start the mock telemetry server.

## Installation

1. Install [uv](https://docs.astral.sh/uv/#installation) on machine for project dependency management.

2. Clone the repository:
   ```bash
   git clone https://github.com/jafark92/insight-engine
   cd insight-engine
   cd 'insight-engine AUV'
   ```

3. Sync Dependencies & Activate the virtual enviroment :
   ```bash
   uv sync
   source .venv/bin/activate
   ```

4. Install [PgBouncer](https://www.pgbouncer.org/) (Optional for development)
   But As we are using Neon. To use PgBouncer on Neon, we can check the “Pooled connection” box in the connection details widget. Note the -pooler suffix on the endpoint ID in your connection string. [source](https://neon.com/blog/pgbouncer-the-one-with-prepared-statements)

5. Alembic migration i.e sync DB Schema
   ```bash
   alembic upgrade head
   ```

5. Run the app with uvicorn
   ```bash   
   uvicorn app.main:app --reload --port 8000
   ```

6. Next Time if make any schema changes
   ```bash
   alembic revision --autogenerate -m "description for schema change"
   alembic upgrade head
   ```