from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse
from sqlalchemy import create_engine, text

import urllib
import os
from pathlib import Path
import pandas as pd
import datetime, io, json


app = FastAPI()
templates = Jinja2Templates(directory="templates")
BASE_QUERY_DIR = Path("Querys")

# --- Merge Definitions Management Utilities ---
import json
def list_merge_definitions():
    merges_dir = Path("merges")
    if not merges_dir.exists():
        return []
    merges = []
    for fname in os.listdir(merges_dir):
        if fname.endswith(".json"):
            fpath = merges_dir / fname
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                merges.append({
                    "name": fname[:-5],
                    "timestamp": data.get("timestamp", ""),
                    "output_table": data.get("output_table", ""),
                    "definition": data
                })
            except Exception as e:
                merges.append({
                    "name": fname[:-5],
                    "timestamp": "ERROR",
                    "output_table": "ERROR",
                    "definition": None
                })
    merges.sort(key=lambda x: x["timestamp"], reverse=True)
    return merges

# Help/How to Use page
@app.get("/help")
async def help_page(request: Request):
    return templates.TemplateResponse("help.html", {"request": request})

# Home navigation page with connections
@app.get("/merges")
async def merges_list(request: Request):
    merges = list_merge_definitions()
    return templates.TemplateResponse("merges_list.html", {"request": request, "merges": merges})

@app.get("/merges/{merge_name}")
async def merge_view(request: Request, merge_name: str):
    merges_dir = Path("merges")
    fpath = merges_dir / f"{merge_name}.json"
    if not fpath.exists():
        return templates.TemplateResponse("merges_list.html", {"request": request, "merges": list_merge_definitions(), "error": "Merge definition not found."})
    with open(fpath, "r", encoding="utf-8") as f:
        merge_def = json.load(f)
    return templates.TemplateResponse("merge_view.html", {"request": request, "merge": merge_def, "merge_name": merge_name})

@app.post("/merges/{merge_name}/delete")
async def merge_delete(request: Request, merge_name: str):
    merges_dir = Path("merges")
    fpath = merges_dir / f"{merge_name}.json"
    if fpath.exists():
        fpath.unlink()
    return RedirectResponse("/merges", status_code=303)

@app.post("/merges/{merge_name}/rerun")
async def merge_rerun(request: Request, merge_name: str):
    # Load merge definition and re-run the merge (stub for now)
    merges_dir = Path("merges")
    fpath = merges_dir / f"{merge_name}.json"
    if not fpath.exists():
        return templates.TemplateResponse("merges_list.html", {"request": request, "merges": list_merge_definitions(), "error": "Merge definition not found."})
    # TODO: Implement re-run logic for both query and SQLite merges
    return RedirectResponse("/merges", status_code=303)

# Ensure connections table exists in etl_results.db
import sqlite3
def ensure_connections_table():
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        driver TEXT NOT NULL,
        server TEXT NOT NULL,
        database TEXT NOT NULL,
        username TEXT,
        password TEXT,
        authentication TEXT,
        extra TEXT
    )''')
    # Add authentication column if missing
    try:
        c.execute('ALTER TABLE connections ADD COLUMN authentication TEXT')
    except Exception:
        pass
    conn.commit()
    conn.close()
ensure_connections_table()


# Home navigation page with connections
@app.get("/")
def home(request: Request):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT name FROM connections")
    connections = [row[0] for row in c.fetchall()]
    conn.close()
    return templates.TemplateResponse("home.html", {"request": request, "connections": connections})

# Connections management
@app.get("/connections")
def list_connections(request: Request):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT id, name, driver, server, database, username FROM connections")
    connections = c.fetchall()
    conn.close()
    return templates.TemplateResponse("connections.html", {"request": request, "connections": connections})


# New connection form
@app.get("/connections/new")
def new_connection_form(request: Request):
    # Provide authentication options
    auth_options = [
        ("", "SQL or Windows (default)"),
        ("ActiveDirectoryPassword", "Azure AD - Username/Password"),
        ("ActiveDirectoryInteractive", "Azure AD - Interactive Login")
    ]
    return templates.TemplateResponse("new_connection.html", {"request": request, "auth_options": auth_options})

# Edit connection form
@app.get("/connections/{name}/edit")
def edit_connection_form(request: Request, name: str):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT name, driver, server, database, username, password, authentication, extra FROM connections WHERE name = ?", (name,))
    row = c.fetchone()
    conn.close()
    if not row:
        return RedirectResponse(url="/connections", status_code=303)
    auth_options = [
        ("", "SQL or Windows (default)"),
        ("ActiveDirectoryPassword", "Azure AD - Username/Password"),
        ("ActiveDirectoryInteractive", "Azure AD - Interactive Login")
    ]
    return templates.TemplateResponse("edit_connection.html", {"request": request, "conn": row, "auth_options": auth_options})

# Update connection
@app.post("/connections/{name}/edit")
def update_connection(request: Request, name: str, driver: str = Form(...), server: str = Form(...), database: str = Form(...), username: str = Form(None), password: str = Form(None), authentication: str = Form(None), extra: str = Form(None)):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("UPDATE connections SET driver=?, server=?, database=?, username=?, password=?, authentication=?, extra=? WHERE name=?", (driver, server, database, username, password, authentication, extra, name))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/connections", status_code=303)

@app.post("/connections/new")
def create_connection(request: Request, name: str = Form(...), driver: str = Form(...), server: str = Form(...), database: str = Form(...), username: str = Form(None), password: str = Form(None), authentication: str = Form(None), extra: str = Form(None)):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO connections (name, driver, server, database, username, password, authentication, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (name, driver, server, database, username, password, authentication, extra))
        conn.commit()
    except Exception as e:
        conn.close()
        return templates.TemplateResponse("new_connection.html", {"request": request, "error": str(e)})
    conn.close()
    # Create query folder for this connection
    (BASE_QUERY_DIR / name).mkdir(parents=True, exist_ok=True)
    return RedirectResponse(url="/connections", status_code=303)

@app.post("/connections/{name}/delete")
def delete_connection(request: Request, name: str):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("DELETE FROM connections WHERE name = ?", (name,))
    conn.commit()
    conn.close()
    # Optionally remove query folder (not deleting files for safety)
    return RedirectResponse(url="/connections", status_code=303)



# Update the connection string for your environment
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SQLP15\\DB15;"
    "DATABASE=SFISData_Reporting;"
    "Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Run and download Excel directly, show runtime/errors
from fastapi import Form
from fastapi.responses import StreamingResponse
import io

# Add new SQL query form (GET/POST) with connection selection
@app.get("/querys/new")
def new_query_form(request: Request):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT name FROM connections")
    connections = [row[0] for row in c.fetchall()]
    conn.close()
    return templates.TemplateResponse("new_query.html", {"request": request, "connections": connections})

@app.post("/querys/new")
def create_new_query(request: Request, connection_name: str = Form(...), query_name: str = Form(...), query_content: str = Form(...)):
    query_dir = BASE_QUERY_DIR / connection_name
    if not query_dir.exists():
        query_dir.mkdir(parents=True, exist_ok=True)
    if not query_name.lower().endswith('.sql'):
        query_name += '.sql'
    query_path = query_dir / query_name
    if query_path.exists():
        return templates.TemplateResponse("new_query.html", {"request": request, "error": "Query file already exists.", "connections": [connection_name]})
    query_path.write_text(query_content)
    return RedirectResponse(url=f"/querys/{connection_name}/{query_name}", status_code=303)

# List queries for a connection
@app.get("/querys/{connection_name}")
def list_queries(request: Request, connection_name: str):
    query_dir = BASE_QUERY_DIR / connection_name
    if not query_dir.exists():
        query_dir.mkdir(parents=True, exist_ok=True)
    queries = [f.name for f in query_dir.glob("*.sql")]
    return templates.TemplateResponse("query_list.html", {
        "request": request,
        "queries": queries,
        "connection_name": connection_name
    })


# View and edit SQL query
@app.get("/querys/{connection_name}/{query_name}")
def view_query(request: Request, connection_name: str, query_name: str):
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if not query_path.exists():
        raise HTTPException(status_code=404, detail="Query not found")
    query_content = query_path.read_text()
    return templates.TemplateResponse("query_view.html", {
        "request": request,
        "query_name": query_name,
        "query_content": query_content,
        "edit_mode": True,
        "connection_name": connection_name
    })

@app.post("/querys/{connection_name}/{query_name}/edit")
def edit_query(request: Request, connection_name: str, query_name: str, query_content: str = Form(...)):
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if not query_path.exists():
        return templates.TemplateResponse("query_view.html", {"request": request, "query_name": query_name, "query_content": "", "error": "Query not found.", "edit_mode": True, "connection_name": connection_name})
    query_path.write_text(query_content)
    return RedirectResponse(url=f"/querys/{connection_name}/{query_name}", status_code=303)

# Delete SQL file
@app.post("/querys/{connection_name}/{query_name}/delete")
def delete_query(request: Request, connection_name: str, query_name: str):
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if query_path.exists():
        query_path.unlink()
    return RedirectResponse(url=f"/querys/{connection_name}", status_code=303)



# Run and download Excel directly, show runtime/errors (per connection)
def get_engine_for_connection(connection_name):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT driver, server, database, username, password, authentication, extra FROM connections WHERE name = ?", (connection_name,))
    row = c.fetchone()
    conn.close()
    if not row:
        raise Exception(f"Connection '{connection_name}' not found.")
    driver, server, database, username, password, authentication, extra = row
    # Build connection string
    conn_parts = [f"DRIVER={driver}", f"SERVER={server}", f"DATABASE={database}"]
    if authentication:
        conn_parts.append(f"Authentication={authentication}")
        if username:
            conn_parts.append(f"UID={username}")
        if password:
            conn_parts.append(f"PWD={password}")
        # Azure AD requires Encrypt and TrustServerCertificate
        if "Encrypt=" not in (extra or ""):
            conn_parts.append("Encrypt=yes")
        if "TrustServerCertificate=" not in (extra or ""):
            conn_parts.append("TrustServerCertificate=no")
    elif username and password:
        conn_parts.append(f"UID={username}")
        conn_parts.append(f"PWD={password}")
    else:
        conn_parts.append("Trusted_Connection=yes")
    if extra:
        conn_parts.append(extra)
    conn_str = ";".join(conn_parts)
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

@app.post("/run/{connection_name}/{query_name}")
def run_and_download_query(connection_name: str, query_name: str, request: Request):
    import time
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if not query_path.exists():
        return templates.TemplateResponse("results.html", {
            "request": request,
            "error": f"Query '{query_name}' not found."
        })
    query = query_path.read_text()
    start = time.time()
    try:
        engine = get_engine_for_connection(connection_name)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Fetch only a preview for display
            preview_rows = result.fetchmany(20)
            columns = result.keys()
            # For metrics, count total rows efficiently
            # (If possible, use COUNT(*) for SELECTs, else fallback to len of all rows)
            # Try to get total row count without fetching all rows
            try:
                count_query = f"SELECT COUNT(*) FROM ( {query} ) AS subq"
                count_result = conn.execute(text(count_query))
                total_rows = count_result.scalar()
            except Exception:
                # Fallback: count preview only (not accurate for large sets)
                total_rows = len(preview_rows)
        elapsed = time.time() - start
        # Prepare preview for display
        preview_rows_display = [[cell if cell is not None else "" for cell in row] for row in preview_rows]
        # Download link (Excel)
        download_url = f"/run/{connection_name}/{query_name}/download"
        # Optionally, provide a link to save to SQLite and view
        save_url = f"/run_save/{connection_name}/{query_name}"
        # Provide a link to view in SQLite viewer if already saved
        sqlite_table_name = f"result_{connection_name}_{query_name.replace('.', '_')}"
        sqlite_view_url = f"/sqlite/{sqlite_table_name}"
        return templates.TemplateResponse("results.html", {
            "request": request,
            "columns": columns,
            "rows": preview_rows_display,
            "runtime": f"{elapsed:.2f} seconds",
            "total_rows": total_rows,
            "download_url": download_url,
            "save_url": save_url,
            "sqlite_view_url": sqlite_view_url,
            "info": "Preview only. Download or save to view all rows."
        })
    except Exception as e:
        elapsed = time.time() - start
        return templates.TemplateResponse("results.html", {
            "request": request,
            "error": str(e),
            "runtime": f"{elapsed:.2f} seconds"
        })

# Add a download endpoint for Excel (full result, streaming)
@app.get("/run/{connection_name}/{query_name}/download")
def download_query_result(connection_name: str, query_name: str):
    import time
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if not query_path.exists():
        raise HTTPException(status_code=404, detail="Query not found.")
    query = query_path.read_text()
    start = time.time()
    try:
        engine = get_engine_for_connection(connection_name)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
        df = pd.DataFrame(rows, columns=columns)
        output = io.BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        filename = f"{query_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# SQLite Viewer Endpoints
@app.get("/sqlite")
def sqlite_tables(request: Request):
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        return templates.TemplateResponse("sqlite_view.html", {"request": request, "tables": [], "error": "No SQLite DB found."})
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [row[0] for row in c.fetchall()]
    # Show both result_ and merged_ tables
    result_tables = [t for t in all_tables if t.startswith("result_")]
    merged_tables = [t for t in all_tables if t.startswith("merged_")]
    conn.close()
    return templates.TemplateResponse("sqlite_view.html", {
        "request": request,
        "tables": result_tables,
        "merged_tables": merged_tables
    })

# Delete SQLite table
@app.post("/sqlite/{table_name}/delete")
def delete_sqlite_table(request: Request, table_name: str):
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        return RedirectResponse(url="/sqlite", status_code=303)
    # Allow deleting both result_ and merged_ tables
    if not (table_name.startswith("result_") or table_name.startswith("merged_")):
        return RedirectResponse(url="/sqlite", status_code=303)
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    try:
        c.execute(f"DROP TABLE IF EXISTS [{table_name}]")
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    return RedirectResponse(url="/sqlite", status_code=303)

# Download SQLite table as Excel
@app.get("/sqlite/{table_name}/download")
def download_sqlite_table(table_name: str):
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        raise HTTPException(status_code=404, detail="No SQLite DB found.")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM [{table_name}]")
        rows = c.fetchall()
        columns = [desc[0] for desc in c.description]
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=columns)
    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    filename = f"{table_name}.xlsx"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)

from fastapi import Query
@app.get("/sqlite/{table_name}")
def sqlite_table_data(request: Request, table_name: str, page: int = Query(1, ge=1), search: str = Query("")):
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        return templates.TemplateResponse("sqlite_view.html", {"request": request, "tables": [], "error": "No SQLite DB found."})
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    # Get columns
    try:
        c.execute(f"PRAGMA table_info([{table_name}])")
        columns = [row[1] for row in c.fetchall()]
    except Exception as e:
        conn.close()
        return templates.TemplateResponse("sqlite_view.html", {"request": request, "tables": [], "error": str(e)})

    # Build WHERE clause for search
    where = ""
    params = []
    if search:
        like_clauses = [f"CAST([{col}] AS TEXT) LIKE ?" for col in columns]
        where = "WHERE " + " OR ".join(like_clauses)
        params = [f"%{search}%"] * len(columns)

    # Get total row count
    count_sql = f"SELECT COUNT(*) FROM [{table_name}] {where}"
    c.execute(count_sql, params)
    total_rows = c.fetchone()[0]

    # Get paginated rows
    page_size = 20
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * page_size
    sql = f"SELECT * FROM [{table_name}] {where} LIMIT ? OFFSET ?"
    c.execute(sql, params + [page_size, offset])
    preview_rows = c.fetchall()
    preview_rows = [[cell if cell is not None else "" for cell in row] for row in preview_rows]
    conn.close()
    return templates.TemplateResponse("sqlite_view.html", {
        "request": request,
        "columns": columns,
        "rows": preview_rows,
        "table_name": table_name,
        "page": page,
        "total_pages": total_pages,
        "search": search,
        "total_rows": total_rows
    })

# --- MERGE QUERIES FEATURE ---
import re

# Helper: extract aliases from SQL (simple parser)
def extract_aliases(sql):
    # Find all ... AS alias (case-insensitive)
    # Find all ... AS alias (case-insensitive)
    aliases = set(re.findall(r'AS\s+([\w\[\]"\']+)', sql, re.IGNORECASE))
    # Also find all columns in SELECT that do not have AS
    select_match = re.search(r'SELECT(.*?)FROM', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1)
        # Split by comma, strip whitespace
        columns = [c.strip() for c in select_clause.split(',')]
        for col in columns:
            # If col has AS, skip (already handled)
            if re.search(r'AS\s+[\w\[\]"\']+$', col, re.IGNORECASE):
                continue
            # Remove table prefix if present (e.g., LK.ReadDT -> ReadDT)
            if '.' in col:
                col_name = col.split('.')[-1].strip()
            else:
                col_name = col.strip()
            # Remove any brackets or quotes
            col_name = col_name.strip('[]"\'')
            # Remove function calls (e.g., MAX(...))
            if '(' in col_name and ')' in col_name:
                continue
            # Skip empty
            if not col_name:
                continue
            aliases.add(col_name)
    return list(aliases)

@app.get("/merge")
def merge_select_queries(request: Request):
    # List all queries by connection
    all_queries = {}
    for conn_folder in BASE_QUERY_DIR.iterdir():
        if conn_folder.is_dir():
            all_queries[conn_folder.name] = [f.name for f in conn_folder.glob("*.sql")]
    return templates.TemplateResponse("merge_select_queries.html", {"request": request, "all_queries": all_queries})

@app.post("/merge")
@app.post("/merge")
async def merge_select_fields(request: Request):
    form = await request.form()
    selected_queries = form.getlist("selected_queries")
    if not selected_queries:
        # Re-render with error
        all_queries = {}
        for conn_folder in BASE_QUERY_DIR.iterdir():
            if conn_folder.is_dir():
                all_queries[conn_folder.name] = [f.name for f in conn_folder.glob("*.sql")]
        return templates.TemplateResponse("merge_select_queries.html", {"request": request, "all_queries": all_queries, "error": "Please select at least one query."})
    queries = []
    for ref in selected_queries:
        conn, query = ref.split("::", 1)
        sql_path = BASE_QUERY_DIR / conn / query
        sql = sql_path.read_text()
        aliases = extract_aliases(sql)
        queries.append({"conn": conn, "query": query, "aliases": aliases})
    return templates.TemplateResponse("merge_select_fields.html", {"request": request, "queries": queries})

@app.post("/merge/fields")
async def merge_and_save(request: Request):
    form = await request.form()
    # Defensive: If form is empty, show error page
    if not form or (len(form.keys()) == 1 and next(iter(form.keys()), None) == "null"):
        return templates.TemplateResponse(
            "merge_sqlite_result.html",
            {
                "request": request,
                "error": "No data submitted. Please use the UI to select tables and fields for merging.",
                "columns": [],
                "rows": [],
                "table_name": None,
                "page": 1,
                "total_pages": 1,
                "search": "",
                "total_rows": 0
            }
        )
    query_count = int(form.get("query_count", 0))
    merge_info = []
    for i in range(query_count):
        ref = form.get(f"query_ref_{i}")
        field = form.get(f"merge_field_{i}")
        if not ref or not field:
            continue
        conn, query = ref.split("::", 1)
        merge_info.append({"conn": conn, "query": query, "field": field, "ref": ref})
    # Get user choices for duplicates
    keep_columns = {}  # {col: ref}
    for key in form.keys():
        if key.startswith("keep_"):
            col = key[5:]
            keep_columns[col] = form.get(key)
    # Get join type (default to outer)
    join_type = form.get("join_type", "outer")
    # Get merged file name (optional)
    merged_filename = form.get("merged_filename", "").strip()
    import re
    if merged_filename:
        merged_filename = re.sub(r'[^A-Za-z0-9_-]', '_', merged_filename)
        if not merged_filename.startswith("merged_"):
            merged_filename = f"merged_{merged_filename}"
    # Load DataFrames and track columns for duplicate renaming
    import pandas as pd
    dfs = []
    col_sources = {}  # {col: [order0, order1, ...]}
    all_columns = []  # list of lists of columns per df
    for idx, m in enumerate(merge_info):
        engine = get_engine_for_connection(m["conn"])
        sql_path = BASE_QUERY_DIR / m["conn"] / m["query"]
        sql = sql_path.read_text()
        with engine.connect() as conn_:
            df = pd.read_sql_query(sql, conn_)
        dfs.append((df, m["field"], m["ref"]))
        all_columns.append(list(df.columns))
        for col in df.columns:
            col_sources.setdefault(col, []).append(idx)

    # Build new DataFrames with renamed columns for duplicates (improved logic)
    # Always use only the original field name (drop any table/query prefix)
    from collections import defaultdict
    def get_base_colname(col):
        # Remove any prefix like result_SFIS_Last_28_Days_sql_ or table_ etc.
        # Only keep the part after the last underscore if it matches known patterns
        # But if the column is already a simple name, keep as is
        # If col is like 'result_SFIS_Last_28_Days_sql_SalesOrder', get 'SalesOrder'
        # If col is like 'table_SalesOrder', get 'SalesOrder'
        # If col is 'SalesOrder', keep as is
        if '_' in col:
            parts = col.split('_')
            # Heuristic: if last part is not empty, use it
            if parts[-1]:
                return parts[-1]
        return col

    # Gather all base column names and their counts
    all_col_order = []
    for df, _, _ in dfs:
        all_col_order.extend([get_base_colname(c) for c in df.columns])
    col_occurrences = defaultdict(int)
    for col in all_col_order:
        col_occurrences[col] += 1
    # For each df, rename columns as needed
    renamed_dfs = []
    col_seen = defaultdict(int)
    for idx, (df, field, ref) in enumerate(dfs):
        rename_map = {}
        for col in df.columns:
            base_col = get_base_colname(col)
            if col_occurrences[base_col] == 1:
                rename_map[col] = base_col  # unique, always use base name
                continue
            col_seen[base_col] += 1
            if col_seen[base_col] == 1:
                rename_map[col] = base_col  # first occurrence, base name
            elif col_seen[base_col] == 2:
                rename_map[col] = f'_{base_col}'
            else:
                rename_map[col] = f'{col_seen[base_col]-1}_{base_col}'
        renamed_dfs.append((df.rename(columns=rename_map), field, ref))

    # Merge DataFrames
    merged = renamed_dfs[0][0]
    for idx, (df, field, ref) in enumerate(renamed_dfs[1:], 1):
        merged = pd.merge(
            merged,
            df,
            left_on=merge_info[0]["field"],
            right_on=field,
            how=join_type  # Use user-selected join type
        )
    # Save merged result as Excel, CSV, and to SQLite under Querys/MERGED/
    import pandas as pd, io, json
    merged_dir = BASE_QUERY_DIR / "MERGED"
    merged_dir.mkdir(exist_ok=True)
    if merged_filename:
        filename = merged_filename
    else:
        filename = f"merged_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    excel_path = merged_dir / f"{filename}.xlsx"
    csv_path = merged_dir / f"{filename}.csv"
    merged.to_excel(excel_path, index=False)
    merged.to_csv(csv_path, index=False)

    # Save to SQLite as a new table
    sqlite_path = Path("etl_results.db")
    table_name = filename
    conn_sqlite = sqlite3.connect(sqlite_path)
    merged.to_sql(table_name, conn_sqlite, if_exists='replace', index=False)
    conn_sqlite.close()

    # Save merge definition as JSON
    merges_dir = Path("merges")
    merges_dir.mkdir(exist_ok=True)
    merge_def = {
        "timestamp": datetime.datetime.now().isoformat(),
        "tables": [
            {"conn": m["conn"], "query": m["query"], "field": m["field"]} for m in merge_info
        ],
        "output_table": table_name
    }
    json_path = merges_dir / f"{filename}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(merge_def, f, indent=2)

    # Prepare preview: replace None/NaN with empty string for display
    all_rows = merged.values.tolist()
    columns = list(merged.columns)
    page_size = 20
    page = 1
    search = ""
    total_rows = len(all_rows)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    # If no rows, show error
    if not all_rows or not columns:
        return templates.TemplateResponse(
            "merge_sqlite_result.html",
            {
                "request": request,
                "error": "No rows returned from merge. Please check your join fields and data.",
                "columns": [],
                "rows": [],
                "table_name": None,
                "page": page,
                "total_pages": total_pages,
                "search": search,
                "total_rows": total_rows
            }
        )
    # Normal preview
    preview_rows = all_rows[:20]
    preview_rows = [[cell if cell is not None else "" for cell in row] for row in preview_rows]
    return templates.TemplateResponse(
        "merge_sqlite_result.html",
        {
            "request": request,
            "table_name": table_name,
            "columns": columns,
            "rows": preview_rows,
            "page": page,
            "total_pages": total_pages,
            "search": search,
            "total_rows": total_rows
        }
    )

# --- MERGE EXISTING SQLITE TABLES FEATURE ---
from fastapi.responses import StreamingResponse
import json

# Step 1: Select tables to merge
@app.get("/merge_sqlite")
def merge_sqlite_select_tables(request: Request):
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall() if row[0].startswith("result_") or row[0].startswith("merged_")]
    conn.close()
    return templates.TemplateResponse("merge_sqlite_select_tables.html", {"request": request, "tables": tables})

# Step 2: Select merge fields
@app.post("/merge_sqlite/select_fields")
async def merge_sqlite_select_fields(request: Request):
    form = await request.form()
    selected_tables = form.getlist("selected_tables")
    if not selected_tables:
        sqlite_path = Path("etl_results.db")
        conn = sqlite3.connect(sqlite_path)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in c.fetchall() if row[0].startswith("result_") or row[0].startswith("merged_")]
        conn.close()
        return templates.TemplateResponse("merge_sqlite_select_tables.html", {"request": request, "tables": tables, "error": "Please select at least one table."})
    # Get columns for each table
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    tables_info = []
    for t in selected_tables:
        c.execute(f'PRAGMA table_info({t})')
        columns = [row[1] for row in c.fetchall()]
        tables_info.append({"name": t, "columns": columns})
    conn.close()
    return templates.TemplateResponse("merge_sqlite_select_fields.html", {"request": request, "tables": tables_info})

# --- Helper: Find duplicate columns across tables ---
def find_duplicate_columns_across_tables(conn, merge_info):
    # Returns: {col_name: [table1, table2, ...]} for columns that appear in more than one table
    col_table_map = {}
    for m in merge_info:
        t = m["table"]
        c = conn.cursor()
        c.execute(f'PRAGMA table_info({t})')
        t_cols = [row[1] for row in c.fetchall()]
        for col in t_cols:
            col_table_map.setdefault(col, []).append(t)
    return {col: tbls for col, tbls in col_table_map.items() if len(tbls) > 1}

# --- Step 2.5: Ask user which duplicate columns to keep ---
from fastapi import Form
@app.post("/merge_sqlite/select_duplicates")
async def merge_sqlite_select_duplicates(request: Request):
    form = await request.form()
    table_count = int(form.get("table_count", 0))
    merge_info = []
    for i in range(table_count):
        ref = form.get(f"table_ref_{i}")
        field = form.get(f"merge_field_{i}")
        if not ref or not field:
            continue
        merge_info.append({"table": ref, "field": field})
    if not merge_info:
        return RedirectResponse(url="/merge_sqlite", status_code=303)
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    # Find duplicate columns
    duplicates = find_duplicate_columns_across_tables(conn, merge_info)
    if not duplicates:
        # No duplicates, proceed to merge
        # Reuse the merge_sqlite_and_save logic
        from starlette.requests import Request as StarletteRequest
        req = request if isinstance(request, StarletteRequest) else request._request
        return await merge_sqlite_and_save(req)
    # For each duplicate, get a preview of values from each table
    previews = {}
    for col, tbls in duplicates.items():
        previews[col] = {}
        for t in tbls:
            c = conn.cursor()
            c.execute(f'SELECT [{col}] FROM [{t}] LIMIT 5')
            previews[col][t] = [row[0] for row in c.fetchall()]
    conn.close()
    return templates.TemplateResponse("merge_sqlite_select_duplicates.html", {
        "request": request,
        "merge_info": merge_info,
        "duplicates": duplicates,
        "previews": previews,
        "table_count": table_count
    })

# --- Update merge_sqlite_and_save to accept user choices for duplicates ---
@app.post("/merge_sqlite/fields")
async def merge_sqlite_and_save(request: Request):
    # Support pagination and search for preview
    form = await request.form()
    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    print("[DEBUG] /merge_sqlite/fields form data:")
    pp.pprint(dict(form))
    # Defensive: If form is empty, show error page
    if not form or (len(form.keys()) == 1 and next(iter(form.keys()), None) == "null"):
        return templates.TemplateResponse(
            "merge_sqlite_result.html",
            {
                "request": request,
                "error": "No data submitted. Please use the UI to select tables and fields for merging.",
                "columns": [],
                "rows": [],
                "table_name": None,
                "page": 1,
                "total_pages": 1,
                "search": "",
                "total_rows": 0
            }
        )
    # Get pagination and search params
    page = int(form.get("page", 1))
    search = form.get("search", "").strip()
    page_size = 20
    table_count = int(form.get("table_count", 0))
    merge_info = []
    # Build a mapping of lowercase column names to actual column names for each table
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    table_col_map = {}  # {table: {lower_col: real_col}}
    for i in range(table_count):
        ref = form.get(f"table_ref_{i}")
        field = form.get(f"merge_field_{i}")
        if not ref or not field:
            continue
        c.execute(f'PRAGMA table_info({ref})')
        col_map = {row[1].lower(): row[1] for row in c.fetchall()}
        table_col_map[ref] = col_map
        real_field = col_map.get(field.lower(), field)
        merge_info.append({"table": ref, "field": real_field})
    print("[DEBUG] merge_info:")
    pp.pprint(merge_info)
    # Get user choices for duplicates
    keep_columns = {}  # {col: table}
    for key in form.keys():
        if key.startswith("keep_"):
            col = key[5:]
            keep_columns[col] = form.get(key)
    print("[DEBUG] keep_columns:")
    pp.pprint(keep_columns)
    base_table = merge_info[0]["table"]
    base_field = merge_info[0]["field"]
    # Gather all columns and their sources (order)
    table_cols = []  # list of (table, [cols])
    col_sources = {}  # {col: [order0, order1, ...]}
    for idx, m in enumerate(merge_info):
        t = m["table"]
        c.execute(f'PRAGMA table_info({t})')
        t_cols = [row[1] for row in c.fetchall()]
        table_cols.append((t, t_cols))
        for col in t_cols:
            col_sources.setdefault(col, []).append(idx)
    print("[DEBUG] table_cols:")
    pp.pprint(table_cols)
    print("[DEBUG] col_sources:")
    pp.pprint(col_sources)
    # Get join type (default to outer)
    join_type = form.get("join_type", "outer")
    # Get merged file name (optional)
    merged_filename = form.get("merged_filename", "").strip()
    import re
    if merged_filename:
        merged_filename = re.sub(r'[^A-Za-z0-9_-]', '_', merged_filename)
        if not merged_filename.startswith("merged_"):
            merged_filename = f"merged_{merged_filename}"
    # --- Improved column naming for SQL-based merge (match DataFrame logic) ---
    # Gather all columns from all tables, track occurrences
    all_cols = []  # list of all columns in order of appearance
    table_cols = []  # list of (table, [cols])
    col_sources = {}  # {col: [order0, order1, ...]}
    for idx, m in enumerate(merge_info):
        t = m["table"]
        c.execute(f'PRAGMA table_info({t})')
        t_cols = [row[1] for row in c.fetchall()]
        table_cols.append((t, t_cols))
        for col in t_cols:
            all_cols.append(col)
            col_sources.setdefault(col, []).append(idx)
    from collections import defaultdict
    col_occurrences = defaultdict(int)
    for col in all_cols:
        col_occurrences[col] += 1
    # For each table, build a rename map for its columns
    renamed_table_cols = []  # list of (table, [renamed_cols])
    col_seen = defaultdict(int)
    for idx, (t, t_cols) in enumerate(table_cols):
        renamed_cols = []
        for col in t_cols:
            if col_occurrences[col] == 1:
                renamed_cols.append((col, col))
            else:
                col_seen[col] += 1
                if col_seen[col] == 1:
                    renamed_cols.append((col, col))
                elif col_seen[col] == 2:
                    renamed_cols.append((col, f'_{col}'))
                else:
                    renamed_cols.append((col, f'{col_seen[col]-1}_{col}'))
        renamed_table_cols.append((t, renamed_cols))
    # Build select columns with aliases
    select_cols = []
    for t, renamed_cols in renamed_table_cols:
        for orig_col, new_col in renamed_cols:
            if orig_col == new_col:
                select_cols.append(f'{t}.[{orig_col}]')
            else:
                select_cols.append(f'{t}.[{orig_col}] AS [{new_col}]')
    join_word = "LEFT JOIN" if join_type == "outer" else "INNER JOIN"
    join_clauses = []
    for idx in range(1, len(merge_info)):
        t = merge_info[idx]["table"]
        f = merge_info[idx]["field"]
        join_clauses.append(f'{join_word} {t} ON {base_table}.[{base_field}] = {t}.[{f}]')
    join_sql = f'CREATE TABLE IF NOT EXISTS [{merged_filename}] AS SELECT {', '.join(select_cols)} FROM {base_table} {' '.join(join_clauses)}'
    # Execute SQL
    sqlite_path = Path("etl_results.db")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    # Drop table if exists
    c.execute(f'DROP TABLE IF EXISTS [{merged_filename}]')
    c.execute(join_sql)
    conn.commit()
    # Save merge definition as JSON
    merges_dir = Path("merges")
    merges_dir.mkdir(exist_ok=True)
    merge_def = {
        "timestamp": datetime.datetime.now().isoformat(),
        "tables": merge_info,
        "output_table": merged_filename
    }
    json_path = merges_dir / f"{merged_filename}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(merge_def, f, indent=2)
    conn.close()
    # Redirect to SQLite viewer for the new merged table
    return RedirectResponse(url=f"/sqlite/{merged_filename}", status_code=303)


# --- Streaming CSV download for any SQLite table ---
@app.get("/sqlite/{table_name}/download_csv_stream")
def download_sqlite_table_stream(table_name: str):
    import csv
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        raise HTTPException(status_code=404, detail="No SQLite DB found.")
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute(f"SELECT * FROM [{table_name}]")
    columns = [desc[0] for desc in c.description]
    def generate():
        yield ','.join(columns) + '\n'
        for row in c:
            yield ','.join([str(cell) if cell is not None else '' for cell in row]) + '\n'
    headers = {"Content-Disposition": f"attachment; filename={table_name}.csv"}
    return StreamingResponse(generate(), media_type="text/csv", headers=headers)

# --- Native SQLite CLI CSV Export (FAST) ---
@app.get("/sqlite/{table_name}/download_csv_native")
def download_sqlite_table_native_csv(table_name: str):
    import subprocess
    from fastapi.responses import StreamingResponse
    import shlex
    db_path = os.path.abspath("etl_results.db")
    # Windows: use sqlite3.exe, must be in PATH or same dir
    sqlite3_exe = "sqlite3.exe" if os.name == "nt" else "sqlite3"
    # Compose the command: .headers on, .mode csv, select *
    sql = f".headers on\n.mode csv\nSELECT * FROM [{table_name}];\n"
    cmd = [sqlite3_exe, db_path]
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="sqlite3.exe not found. Please ensure it is in your PATH.")
    def stream():
        try:
            out, err = proc.communicate(input=sql.encode("utf-8"))
            if proc.returncode != 0:
                yield f"Error: {err.decode('utf-8')}"
            else:
                yield out.decode("utf-8", errors="replace")
        finally:
            proc.stdout.close()
            proc.stderr.close()
    headers = {"Content-Disposition": f"attachment; filename={table_name}_native.csv"}
    return StreamingResponse(stream(), media_type="text/csv", headers=headers)

@app.post("/run_save/{connection_name}/{query_name}")
def run_and_save_query_to_sqlite(connection_name: str, query_name: str, request: Request):
    import time
    query_path = BASE_QUERY_DIR / connection_name / query_name
    if not query_path.exists():
        return templates.TemplateResponse("results.html", {
            "request": request,
            "error": f"Query '{query_name}' not found."
        })
    query = query_path.read_text()
    start = time.time()
    try:
        engine = get_engine_for_connection(connection_name)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
        # Save to SQLite
        df = pd.DataFrame(rows, columns=columns)
        # Convert decimal.Decimal columns to float (for SQLite compatibility)
        import decimal
        for col in df.columns:
            if df[col].dtype == 'object' and any(isinstance(x, decimal.Decimal) for x in df[col] if x is not None):
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        sqlite_path = Path("etl_results.db")
        table_name = f"result_{connection_name}_{query_name.replace('.', '_')}"
        conn_sqlite = sqlite3.connect(sqlite_path)
        df.to_sql(table_name, conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        elapsed = time.time() - start
        # Redirect to SQLite viewer for the new table
        return RedirectResponse(url=f"/sqlite/{table_name}", status_code=303)
    except Exception as e:
        elapsed = time.time() - start
        return templates.TemplateResponse("results.html", {
            "request": request,
            "error": str(e),
            "runtime": f"{elapsed:.2f} seconds"
        })

from fastapi import Body

@app.post("/sqlite/{table_name}/rename_column")
def rename_sqlite_column(table_name: str, request: Request, old_name: str = Form(...), new_name: str = Form(...)):
    sqlite_path = Path("etl_results.db")
    if not sqlite_path.exists():
        return JSONResponse({"success": False, "error": "No SQLite DB found."})
    if not old_name or not new_name:
        return JSONResponse({"success": False, "error": "Missing column name(s)."})
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    # Get current columns
    c.execute(f'PRAGMA table_info([{table_name}])')
    columns = [row[1] for row in c.fetchall()]
    if new_name in columns:
        conn.close()
        return JSONResponse({"success": False, "error": f"Column '{new_name}' already exists."})
    try:
        c.execute(f'ALTER TABLE [{table_name}] RENAME COLUMN [{old_name}] TO [{new_name}]')
        conn.commit()
        conn.close()
        return JSONResponse({"success": True})
    except Exception as e:
        conn.close()
        return JSONResponse({"success": False, "error": str(e)})