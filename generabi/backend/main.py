from fastapi import FastAPI, Body, File, UploadFile, Form
from pydantic import BaseModel
import pyodbc
import requests
import os
import uvicorn
import pandas as pd
import time
from typing import List
import io
import graphviz
from fastapi import Response

# =============================================================================
# Configurazioni e Variabili Globali
# =============================================================================
# Nota: Queste variabili sono usate a scopo dimostrativo. In un ambiente di produzione,
# dovresti gestire le sessioni, la persistenza e la sicurezza in maniera appropriata.

app = FastAPI()

# Salvataggio delle configurazioni del database per l'utente corrente
user_db_config = {}

# Salvataggio in memoria dei metadati delle tabelle e delle relazioni
metadata_store = {}
relationships_store = []

# DataFrames caricati (file upload)
uploaded_dataframes = {}
csv_metadata = {}

# Salvataggio dei risultati delle query per ogni sessione (da sostituire con un meccanismo di sessione sicuro)
session_results = {}
session_id = "unique_for_user"  # In produzione, usa un ID generato per ogni utente


# =============================================================================
# Modelli Pydantic (Validazione dei dati in ingresso)
# =============================================================================
class Relationship(BaseModel):
    table1: str
    column1: str
    table2: str
    column2: str
    type: str  # Esempio: "1:1", "1:N", etc.

class DBConfig(BaseModel):
    server: str
    database: str
    username: str
    password: str

class ChatMessage(BaseModel):
    message: str

class TablesRequest(BaseModel):
    tables: List[str]


# =============================================================================
# Endpoint di Health Check
# =============================================================================
@app.get("/health")
def health_check():
    """Verifica che il server FastAPI sia in esecuzione."""
    return {"status": "ok", "message": "FastAPI running"}


# =============================================================================
# Sezione 1: Upload File e Visualizzazione Anteprime
# =============================================================================
@app.post("/uploadfile")
async def upload_file(
    file: UploadFile = File(...),
    delimiter: str = Form(",")  # Riceve il delimitatore tramite form, default: ","
):
    """
    Carica un file CSV/TSV, lo legge in un DataFrame (usando il delimitatore specificato)
    e salva il DataFrame in memoria.
    Restituisce anche le prime righe del DataFrame per una preview.
    """
    try:
        # Legge il contenuto del file
        content = await file.read()
        # Crea un DataFrame dal contenuto del file
        df = pd.read_csv(io.BytesIO(content), delimiter=delimiter, encoding="utf-8")
        # Salva il DataFrame in una variabile globale
        uploaded_dataframes[file.filename] = df

        # Costruiamo i metadati
        columns_info = []
        for col in df.columns:
            # Tipo dedotto da Pandas (es. int64, float64, object, ecc.)
            dtype_str = str(df[col].dtype)
            # Se vuoi, puoi tentare di calcolare la lunghezza massima di stringhe
            # se dtype è 'object', e se df[col] sono tutte stringhe
            # Qui per semplicità lasciamo max_length = None
            columns_info.append({
                "column_name": col,
                "data_type": dtype_str,
                "max_length": None
            })
        # Rimuoviamo l'estensione dal filename, es. "mydata.csv" -> ("mydata", ".csv")
        import os
        filename_noext, _ = os.path.splitext(file.filename)
        
        # Salviamo i metadati nel dizionario globale
        csv_metadata[filename_noext] = columns_info
        print(csv_metadata)

        return {
            "status": "ok",
            "filename": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "head": df.head(5).to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "details": str(e)}
    
@app.get("/get_csv_metadata")
def get_csv_metadata(filename: str):
    """
    Restituisce i metadati per un file CSV specificato.
    
    Il parametro 'filename' deve essere il nome del file senza estensione.
    Se i metadati sono stati salvati al momento dell'upload, vengono restituiti,
    altrimenti viene restituito un messaggio di errore.
    """
    if filename in csv_metadata:
        return {"status": "ok", "metadata": csv_metadata[filename]}
    else:
        return {"status": "error", "details": f"Nessun metadato trovato per il file '{filename}'."}

@app.get("/list_files")
def list_files():
    """
    Restituisce la lista dei file caricati con una preview (prime 10 righe)
    per ciascun DataFrame.
    """
    print('entrato list files')
    try:
        files_info = []
        for fname, df in uploaded_dataframes.items():
            files_info.append({
                "filename": fname,
                "head": df.head(10).to_dict(orient="records")
            })
        return {"status": "ok", "files": files_info}
    except Exception as e:
        return {"status": "error", "details": str(e)}


# =============================================================================
# Sezione 2: Connessione al Database e Interrogazione
# =============================================================================
@app.post("/connect_db")
def connect_db(config: DBConfig):
    """
    Salva le credenziali del database e tenta la connessione a SQL Server.
    Se la connessione ha successo, restituisce un messaggio di conferma.
    """
    global user_db_config
    user_db_config = {
        "server": config.server,
        "database": config.database,
        "username": config.username,
        "password": config.password
    }

    max_retry_attempts = 3
    n_iteration = 1

    while n_iteration <= max_retry_attempts:
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={config.server};"
                f"DATABASE={config.database};"
                f"UID={config.username};"
                f"PWD={config.password}"
            )
            # Tenta la connessione
            conn = pyodbc.connect(connection_string)
            conn.close()
            return {
                "status": "ok",
                "message": "Connessione avvenuta con successo",
                "details": user_db_config,
                "iterations": n_iteration
            }
        except Exception as e:
            print(f"Attempt {n_iteration}: Connection Failed, trying again in 5 seconds...")
            time.sleep(5)
            n_iteration += 1
            if n_iteration == 4:
                return {
                    "status": "error",
                    "message": str(e),
                    "details": user_db_config,
                    "iterations": n_iteration
                }

@app.get("/list_tables")
def list_tables():
    """
    Restituisce la lista di tabelle e viste presenti nel database
    utilizzando la vista INFORMATION_SCHEMA.TABLES.
    """
    global user_db_config
    if not user_db_config:
        return {"status": "error", "details": "No DB config found. Connect first."}
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={user_db_config['server']};"
            f"DATABASE={user_db_config['database']};"
            f"UID={user_db_config['username']};"
            f"PWD={user_db_config['password']};"
        )
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            rows = cursor.fetchall()
            objects = []
            for row in rows:
                objects.append({
                    "schema": row[0],
                    "name": row[1],
                    "type": row[2]
                })
        return {"status": "ok", "tables": objects}
    except Exception as e:
        return {"status": "error", "details": str(e)}

@app.post("/get_table_metadata")
def get_table_metadata(req: TablesRequest):
    """
    Riceve una lista di tabelle (nel formato "schema.tabella") e restituisce i metadati
    (nome colonna, tipo e lunghezza) per ciascuna di esse.
    """
    global user_db_config, metadata_store
    if not user_db_config:
        return {"status": "error", "details": "No DB config found. Connect first."}
    
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={user_db_config['server']};"
        f"DATABASE={user_db_config['database']};"
        f"UID={user_db_config['username']};"
        f"PWD={user_db_config['password']};"
    )

    try:
        metadata = {}
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for full_name in req.tables:
                schema, table_name = full_name.split(".", 1)
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? 
                      AND TABLE_NAME = ?
                """, (schema, table_name))
                rows = cursor.fetchall()
                cols_info = []
                for col_name, data_type, char_len in rows:
                    cols_info.append({
                        "column_name": col_name,
                        "data_type": data_type,
                        "max_length": char_len
                    })
                metadata[full_name] = cols_info
        
        # Salva i metadati per uso futuro
        metadata_store["last_selected_tables"] = req.tables
        metadata_store["metadata"] = metadata
        return {"status": "ok", "metadata": metadata}
    except Exception as e:
        return {"status": "error", "details": str(e)}


# =============================================================================
# Sezione 3: Gestione delle Relazioni tra Tabelle
# =============================================================================
@app.post("/store_relationships")
def store_relationships(rels: List[Relationship]):
    """
    Riceve una lista di relazioni (definite dall'utente) e le salva in memoria.
    In un sistema reale potresti salvarle in un database o in un file.
    """
    global relationships_store
    relationships_store = rels  # Sovrascrive le relazioni precedenti
    print(relationships_store)
    return {
        "status": "ok",
        "message": "Relazioni memorizzate",
        "count": len(rels)
    }




from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o la lista di origin ammessi
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



def sanitize(s: str) -> str:
    return s.replace(".", "_").replace(" ", "_")

# Dizionari globali da costruire durante la generazione del diagramma
node_ids = {}    # es: { "dbo.calendar": "dbo_calendar", "csv.file_predictions": "csv_file_predictions" }
port_map = {}    # es: { ("dbo.calendar", "date"): "dbo_calendar_date", ... }

def get_columns(table_name: str):
    """
    Ritorna una lista di dict {column_name, data_type, max_length} 
    per la tabella/CSV specificata.
    """
    # 1) Se esiste in metadata_store (tabelle DB)
    if table_name in metadata_store["metadata"]:
        return metadata_store["metadata"][table_name]

    # 2) Se inizia con "csv." o "File CSV."
    if "." in table_name:
        # Esempio: "csv.file_predictions" -> schema="csv", csv_name="file_predictions"
        schema, csv_name = table_name.split(".", 1)
        if csv_name in csv_metadata:
            return csv_metadata[csv_name]

    # 3) Fallback: se non trovi nulla, restituisci lista vuota
    return []

@app.get("/model_diagram", response_class=Response)
def model_diagram():
    print('entrato')
    """
    Genera un diagramma SVG del modello dati (tabelle DB e file CSV),
    mostrando in verticale le colonne con HTML labels.
    """
    if not relationships_store:
        empty_svg = (
            "<svg xmlns='http://www.w3.org/2000/svg' width='300' height='50'>"
            "<text x='10' y='20'>Nessuna relazione definita</text></svg>"
        )
        return Response(content=empty_svg, media_type="image/svg+xml")

    dot = graphviz.Digraph(comment="Data Model", format='svg')
    dot.attr(rankdir='LR')  # se preferisci i nodi da sinistra a destra

    # Puliamo i dizionari (nel caso questa funzione venga chiamata più volte)
    node_ids.clear()
    port_map.clear()

    # 1) Raccogliamo tutti i nomi di “tabelle” (incluse CSV)
    tables_set = set()
    for rel in relationships_store:
        tables_set.add(rel.table1)
        tables_set.add(rel.table2)

    # 2) Crea i nodi con HTML label e PORT per ogni colonna
    for table_name in tables_set:
        # Genera un id “pulito” per il nodo
        node_id = sanitize(table_name)
        node_ids[table_name] = node_id

        columns_info = get_columns(table_name)

        # Costruiamo le righe della tabella HTML, con <TD PORT="...">
        rows_html = ""
        for col in columns_info:
            col_name = col["column_name"]
            # Genera un port id univoco
            port_id = sanitize(f"{table_name}_{col_name}")
            port_map[(table_name, col_name)] = port_id

            # Esempio: <TR><TD PORT="dbo_calendar_date">date : datetime</TD></TR>
            col_label = f"{col_name} : {col.get('data_type', '')}"
            rows_html += f'<TR><TD PORT="{port_id}" ALIGN="LEFT">{col_label}</TD></TR>'

        # Tabella HTML con header colorato
        label_html = f"""<
        <TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR>
                <TD BGCOLOR="#F2DF91" COLSPAN="1"><B>{table_name}</B></TD>
            </TR>
            {rows_html}
        </TABLE>
        >"""

        # Creiamo il nodo con shape="none" e l'etichetta HTML
        dot.node(node_id, label=label_html, shape="none")

    # 3) Aggiungi gli archi, collegando i port corrispondenti
    for rel in relationships_store:
        # Esempio: rel.table1 = "dbo.calendar", rel.column1 = "date", rel.type = "1:N"
        # Ricava i node_id e port_id
        node1 = node_ids[rel.table1]
        port1 = port_map.get((rel.table1, rel.column1), None)

        node2 = node_ids[rel.table2]
        port2 = port_map.get((rel.table2, rel.column2), None)

        # Se i port sono validi, crea l'arco
        if port1 and port2:
            # Metti come etichetta solo la cardinalità (es. "1:N")
            dot.edge(f"{node1}:{port1}", f"{node2}:{port2}", label=rel.type)
        else:
            # Se manca qualche colonna, magari crea un edge generico
            dot.edge(node1, node2, label=f"{rel.type} (colonna non trovata)")

    svg_data = dot.pipe(format='svg')
    return Response(content=svg_data, media_type="image/svg+xml")


@app.get("/list_columns")
def list_columns(table_name: str):
    """
    (Stub) Restituisce le colonne di una tabella specificata.
    Da implementare: ad esempio, eseguendo una query su INFORMATION_SCHEMA.COLUMNS.
    """
    pass

@app.post("/save_schema")
def save_schema(schema: dict = Body(...)):
    """
    Riceve lo schema definito dall'utente (tabelle e join) e lo salva in memoria.
    Questo schema potrà essere usato per fornire contesto al LLM.
    """
    # Esempio: memorizza lo schema in una variabile globale o in un database
    user_schema = schema
    return {"status": "ok", "message": "Schema salvato."}

# def generate_schema_description(user_schema: dict) -> str:
#     """
#     Trasforma il dizionario user_schema in una stringa descrittiva da passare all'LLM.
#     """
#     result_lines = []
#     tables_info = user_schema.get("tables", {})
#     for table_name, table_details in tables_info.items():
#         result_lines.append(f"Table: {table_name}\n")
#         columns = table_details.get("columns", {})
#         if columns:
#             result_lines.append("  Columns:")
#             for col_name, col_type in columns.items():
#                 result_lines.append(f"    - {col_name} ({col_type})")
#         relationships = table_details.get("relationships", {})
#         if relationships:
#             result_lines.append("  Relationships:")
#             for related_table, join_condition in relationships.items():
#                 result_lines.append(f"    - {related_table}: {join_condition}")
#         result_lines.append("")  # Riga vuota per separare le tabelle
#     schema_description = "\n".join(result_lines)
#     return schema_description

# Funzione per generare una descrizione testuale dello schema dati
def generate_schema_description() -> str:
    description = "Schema del modello dati:\n"
    # Aggiungi le tabelle del database
    for table, columns in metadata_store.get("metadata", {}).items():
        col_desc = ", ".join([f"{col['column_name']} ({col['data_type']})" for col in columns])
        description += f"Tabella {table}: {col_desc}.\n"
    # Aggiungi i file CSV
    for csv_name, columns in csv_metadata.items():
        col_desc = ", ".join([f"{col['column_name']} ({col['data_type']})" for col in columns])
        description += f"File CSV {csv_name}: {col_desc}.\n"
    return description


# =============================================================================
# Sezione 4: Integrazione con LLM e Chat SQL
# =============================================================================

def call_llm_to_generate_sql(prompt: str) -> str:
    openai_api_key = "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" 
    #os.getenv("OPENAI_API_KEY", "CHANGEME")
    ## open_api_key has been disabled before publishing
    headers = {"Authorization": f"Bearer {openai_api_key}"}
    url = "https://api.openai.com/v1/responses"
    payload = {
        "model": "gpt-4o-mini",
        "input": prompt,
        "max_output_tokens": 1500,
        "temperature": 0.2
    }
    resp = requests.post(url, headers=headers, json=payload)
    
    if resp.status_code == 200:
        raw_text = resp.json()["output"][0]["content"][0]["text"]
        sql_query = raw_text.strip()
        return sql_query
    else:
        return f"-- Errore generando query: {resp.text}"

# Funzione per eseguire una query SQL sul database
def run_sql(query: str) -> dict:
    # Se non c'è configurazione DB, restituisce un errore
    # In una versione avanzata dovresti anche gestire i casi CSV, ad esempio leggendo dal DataFrame in memoria
    try:
        # Costruisci la stringa di connessione 
        global user_db_config
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"Server={user_db_config['server']};"
            f"Database={user_db_config['database']};"
            f"Uid={user_db_config['username']};"
            f"Pwd={user_db_config['password']};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            print(rows)
            results = []
            for row in rows:
                results.append({columns[i]: str(row[i]) for i in range(len(columns))})
            return {"columns": columns, "rows": results}
    except Exception as e:
        return {"error": str(e)}

def drop_backticks_and_sql(text: str) -> str:
    # Rimuove tutti i backtick
    without_backticks = text.replace('`', '')
    # Rimuove tutte le occorrenze di "sql" (case-sensitive)
    result = without_backticks.replace('sql', '')
    return result


@app.post("/chat")
def chat_with_llm(chat_msg: ChatMessage):
    """
    Riceve una richiesta in linguaggio naturale dall'utente relativa ad un'analisi dei dati.
    Utilizza il modello dati (schema) per generare un prompt contestualizzato per l'LLM,
    che genera una query SQL; la query viene eseguita e il risultato restituito.
    """
    user_text = chat_msg.message.strip()
    if not user_text:
        return {"error": "Inserisci una richiesta valida."}

    # Genera la descrizione dello schema
    schema_description = generate_schema_description()

    # Costruisci il prompt per l'LLM includendo il contesto dello schema
    prompt = (
        "Sei un assistente esperto in SQL che genera query basate sul modello dati fornito. "
        "Di seguito trovi lo schema del modello dati:\n\n"
        f"{schema_description}\n"
        "L'utente ha chiesto: \"" + user_text + "\"\n\n"
        "Genera SOLO una query SQL valida per rispondere a questa richiesta, senza commenti o spiegazioni. "
        "SOLO la query e basta senza nient'altro del tipo: 'SELECT sum(sales) from [dbo].[order]'"
        "Per favore usa sempre le parentesi quadre per racchiudere il nome dello schema e quello delle tabelle, quindi [dbo].[agent] per esempio"
    )

    generated_sql = call_llm_to_generate_sql(prompt)
    print(generated_sql)
    generated_sql=drop_backticks_and_sql(generated_sql)

    sql_result = run_sql(generated_sql)

    return {
        "sql_generated": generated_sql,
        "results": sql_result
    }

# def call_llm_to_generate_sql(user_text: str) -> str:
#     """
#     Utilizza l'API di OpenAI per generare una query SQL in risposta al messaggio utente.
#     Nota: in un'applicazione reale dovresti includere un prompt contestuale con lo schema.
#     """
#     openai_api_key = os.getenv("OPENAI_API_KEY", "CHANGEME")
#     headers = {"Authorization": f"Bearer {openai_api_key}"}
#     url = "https://api.openai.com/v1/completions"
    
#     # Prompt di esempio: aggiungi lo schema se disponibile
#     prompt = (
#         "Sei un assistente che genera query SQL. Ecco lo schema del db: {schema_str}. "
#         "L'utente dice: '" + user_text + "'. "
#         "Rispondi con SOLO la query SQL, senza commenti né spiegazioni. "
#         "Esempio: SELECT * FROM table WHERE ..."
#     )
#     payload = {
#         "model": "text-davinci-003",
#         "prompt": prompt,
#         "max_tokens": 100,
#         "temperature": 0.2
#     }
#     resp = requests.post(url, headers=headers, json=payload)
#     if resp.status_code == 200:
#         raw_text = resp.json()["choices"][0]["text"]
#         sql_query = raw_text.strip()
#         return sql_query
#     else:
#         return f"-- Errore generando query: {resp.text}"

# def run_sql(query: str):
#     """
#     Esegue una query SQL sul database utilizzando la configurazione salvata.
#     Restituisce il risultato come un dizionario contenente le colonne e le righe.
#     """
#     if not user_db_config:
#         return {"error": "Nessuna configurazione DB presente. Chiama /connect_db prima."}
#     try:
#         conn_str = (
#             f"Driver={{ODBC Driver 17 for SQL Server}};"
#             f"Server={user_db_config['server']};"
#             f"Database={user_db_config['database']};"
#             f"Uid={user_db_config['username']};"
#             f"Pwd={user_db_config['password']};"
#             "Encrypt=yes;"
#             "TrustServerCertificate=no;"
#             "Connection Timeout=30;"
#         )
#         with pyodbc.connect(conn_str) as conn:
#             cursor = conn.cursor()
#             cursor.execute(query)
#             # Se la query restituisce risultati, raccoglie le colonne e le righe
#             columns = [desc[0] for desc in cursor.description] if cursor.description else []
#             rows = cursor.fetchall()
            
#             results = []
#             for row in rows:
#                 row_dict = {columns[i]: str(row[i]) for i in range(len(columns))}
#                 results.append(row_dict)
            
#             # Salva i risultati nella "sessione" (da sostituire con un meccanismo di sessione sicuro)
#             session_results[session_id] = {"columns": columns, "rows": results}
#             return {"columns": columns, "rows": results}
#     except Exception as e:
#         return {"error": str(e)}


# =============================================================================
# Avvio del Server con Uvicorn
# =============================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

