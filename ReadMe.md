# RFID Query App

## Setup Instructions

### 1. Environment Setup
- Requires Python 3.9 or newer.
- Recommended: Use a virtual environment (venv or conda).

```sh
python -m venv venv
venv\Scripts\activate  # On Windows
# or
source venv/bin/activate  # On Linux/Mac
```

### 2. Install Dependencies
Install required packages:

```sh
pip install fastapi uvicorn sqlalchemy pandas openpyxl
```

### 3. Running the App (LAN Access)
To run the app and allow access from other devices on your LAN, use:

```sh
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

- Visit `http://<your-ip>:8000` from any device on your network.
- The main app file is `app.py` (not `main.py`).

### 4. Usage
- Open a browser and go to `http://localhost:8000` or `http://<your-ip>:8000`.
- Use the navigation bar to access all features: connections, queries, SQLite viewer, merging, and help.

### 5. Notes
- Data is stored in `etl_results.db` (SQLite).
- Query files are stored in the `Querys/` folder.
- Merged tables and results are managed automatically.

### 6. Troubleshooting
- If you see errors about missing packages, ensure all dependencies are installed.
- For database connection issues, check your connection settings under Connections.

---

For more help, see the in-app Help page or contact your administrator.
