# insight-engine

`mock-telemetry` for mocking AUVs telemetry data. Run `python mock-telemetry/main.py` using the `insight-engine AUV` virtual environment to start the mock telemetry server.

## Installation

1. Install [uv](https://docs.astral.sh/uv/#installation) on machine for project dependency management.

2. Clone the repository:
   ```bash
   git clone https://github.com/jafark92/insight-engine
   cd insight-engine
   cd 'insight-engine AUV'
   ```
3. Sync Dependencies:
   ```bash
   uv sync
   ```

4. Activate the virtual enviroment and run the app with uvicorn
   ```bash
   source .venv/bin/activate
   uvicorn app.main:app --reload --port 8000
   ```






