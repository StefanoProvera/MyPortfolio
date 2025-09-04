import panel as pn
import requests
import pandas as pd
import uuid
import os
import time

# -----------------------------------------------------------------------------
# Configurazioni iniziali e setup di Panel
# -----------------------------------------------------------------------------
pn.extension(sizing_mode="stretch_width")  # I widget si adattano automaticamente
# BACKEND_URL: Assicurati che l'URL sia corretto per il tuo backend
BACKEND_URL = "http://127.0.0.1:8000"


# =============================================================================
# Sezione 1: Upload File CSV
# =============================================================================
# Widgets per il caricamento del file CSV e per la selezione del delimitatore
delimiter_select = pn.widgets.Select(
    name="Delimitatore CSV",
    options={
        "Virgola (,)": ",",
        "Punto e virgola (;)": ";",
        "Tab (\\t)": "\t",
        "Barra verticale (|)": "|"
    },
    value=","
)
file_input = pn.widgets.FileInput(accept=".csv,.tsv")
upload_button = pn.widgets.Button(name="Carica File", button_type="primary")
upload_output = pn.pane.Markdown("")
upload_output_table = pn.Column()

def upload_file(event):
    """
    Gestisce l'upload del file CSV verso il backend.
    Invia sia il file che il delimitatore come parte della richiesta POST.
    Se la risposta contiene una preview (head), la visualizza in una tabella.
    """
    if file_input.value is None:
        upload_output.object = "Nessun file caricato."
    else:
        files = {"file": (file_input.filename, file_input.value)}
        data = {"delimiter": delimiter_select.value}
        try:
            resp = requests.post(f"{BACKEND_URL}/uploadfile", files=files, data=data)
            answer = resp.json()
            if answer['status'] == 'ok':
                upload_output.object = "File caricato correttamente a backend"
                # Se disponibile, visualizza le prime righe del CSV
                if "head" in answer:
                    df = pd.DataFrame(answer["head"])
                    df_pane = pn.widgets.DataFrame(
                        df,
                        name="Prime righe del CSV",
                        width=600,
                        show_index=False
                    )
                    upload_output_table.clear()
                    upload_output_table.append(
                        pn.Column(
                            pn.pane.Markdown("**Ecco una preview del dataframe caricato**"),
                            df_pane
                        )
                    )
                else:
                    upload_output_table.clear()
            else:
                upload_output.object = f"Errore: {answer.get('message', 'Risposta non prevista')}"
        except Exception as e:
            upload_output.object = f"Errore nell'upload: {e}"

upload_button.on_click(upload_file)

# Layout per l'upload file: file input e selezione delimitatore affiancati
row_layout = pn.Row(
    pn.Column(file_input, sizing_mode="stretch_width"),
    pn.Column(delimiter_select, sizing_mode="fixed", width=200),
    sizing_mode="stretch_width"
)

upload_card = pn.layout.Card(
    pn.pane.Markdown("### Carica files"),
    row_layout,
    upload_button,
    upload_output,
    upload_output_table,
    title="Caricamento CSV",
    collapsible=True,
    header_color="#0d6efd",
    margin=(10, 10, 10, 10)
)


# =============================================================================
# Sezione 2: Visualizza File Caricati
# =============================================================================
refresh_button = pn.widgets.Button(name="Visualizza file caricati", button_type="primary")
files_list_output = pn.Column()

def get_uploaded_files(event):
    """
    Recupera la lista dei file caricati dal backend e visualizza una preview
    per ciascun file (se disponibile).
    """
    try:
        resp = requests.get(f"{BACKEND_URL}/list_files")
        data = resp.json()
        if data.get("status") == "ok":
            files = data.get("files", [])
            if not files:
                files_list_output.objects = [pn.pane.Markdown("**Nessun file caricato**")]
                return
            
            elements = []
            for item in files:
                fname = item["filename"]
                head_data = item["head"]  # lista di dict con le prime righe del file
                df = pd.DataFrame(head_data)
                df_pane = pn.widgets.DataFrame(
                    df, 
                    name=fname,
                    width=600,
                    show_index=False
                )
                elements.append(pn.pane.Markdown(f"### {fname}"))
                elements.append(df_pane)
            files_list_output.objects = elements
        else:
            files_list_output.objects = [pn.pane.Markdown(f"**Errore**: {data.get('details')}")]
    except Exception as e:
        files_list_output.objects = [pn.pane.Markdown(f"**Eccezione**: {e}")]

refresh_button.on_click(get_uploaded_files)

visualizza_file_card = pn.Column(
    refresh_button,
    files_list_output
)


# =============================================================================
# Sezione 3: Configurazione Database e Metadati
# =============================================================================
# Widgets per la connessione al database
db_server = pn.widgets.TextInput(name="Server")
db_database = pn.widgets.TextInput(name="Database")
db_username = pn.widgets.TextInput(name="Username")
db_password = pn.widgets.PasswordInput(name="Password")
db_button = pn.widgets.Button(name="Connetti", button_type="success")
db_output = pn.pane.Markdown("")

# Widgets per visualizzare oggetti del DB (tabelle e viste) in un accordion
show_tables_button = pn.widgets.Button(name="Aggiorna Lista Oggetti", button_type="primary", visible=True)
output_area = pn.pane.Markdown(visible=False)

# Accordion per mostrare oggetti raggruppati per schema
objects_accordion = pn.Accordion(css_classes=["my-accordion"])
checkbox_map = {}  # Mappa per salvare i CheckBoxGroup per ogni schema


def connect_db(event):

    """
    Invia i dati di connessione al backend e aggiorna la visibilità del pulsante
    per caricare le tabelle in caso di connessione avvenuta con successo.
    """
    payload = {
        "server": db_server.value,
        "database": db_database.value,
        "username": db_username.value,
        "password": db_password.value
    }
    try:
        resp = requests.post(f"{BACKEND_URL}/connect_db", json=payload)
        if resp.status_code == 200:
            data = resp.json()
            if data['status'] == 'ok':
                db_output.object = (
                    "Connessione avvenuta con successo!"
                    f"\n Connessione avvenuta dopo {data['iterations']} tentativo/i"
                )
            else:
                db_output.object = f"Errore nella connessione: {data['message']} (Stato: {data['status']})"
        else:
            db_output.object = f"Errore: {resp.text}"
    except Exception as e:
        db_output.object = f"Errore nella connessione: {e}"

db_button.on_click(connect_db)

def show_objects(event):
    """
    Richiede dal backend la lista di tabelle e viste e costruisce un accordion a tre livelli:
      - Primo livello: 'Tabelle', 'Viste' e 'CSV'
      - Secondo livello: per ogni categoria, raggruppa per schema (per CSV si usa una chiave fissa)
      - Terzo livello: un CheckBoxGroup con i nomi degli oggetti
    """
    try:
        # 1) Recupera le tabelle dal DB
        resp = requests.get(f"{BACKEND_URL}/list_tables")
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "ok":
                db_objects = data.get("tables", [])
            else:
                output_area.visible = True
                output_area.object = f"**Errore**: {data.get('details')}"
                return
        else:
            output_area.visible = True
            output_area.object = f"**Errore HTTP**: {resp.text}"
            return

        # 2) Recupera i file CSV caricati
        csv_resp = requests.get(f"{BACKEND_URL}/list_files")
        csv_objects = []  # Lista separata per i CSV
        if csv_resp.status_code == 200:
            csv_data = csv_resp.json()
            if csv_data.get("status") == "ok":
                files_list = csv_data.get("files", [])
                for f in files_list:
                    # Creiamo un oggetto con schema 'csv' e type 'CSV'
                    csv_objects.append({
                        "schema": "csv",
                        "name": os.path.splitext(f["filename"])[0],
                        "type": "CSV"
                    })
            else:
                output_area.visible = True
                output_area.object = f"**Errore**: {csv_data.get('details')}"
                return

        # 3) Separa i db_objects in tabelle e viste
        tables = {}
        views = {}
        for obj in db_objects:
            schema = obj["schema"]
            name = obj["name"]
            objtype = obj["type"]  # "BASE TABLE" o "VIEW"
            if objtype == "BASE TABLE":
                tables.setdefault(schema, []).append(name)
            elif objtype == "VIEW":
                views.setdefault(schema, []).append(name)

        # 4) Per i CSV li raggruppiamo sotto una chiave fissa (ad es. "File CSV")
        csv_hierarchy = {"File CSV": [obj["name"] for obj in csv_objects]}

        # 5) Costruisci la struttura gerarchica finale con 3 categorie
        hierarchy = {
            "Tabelle": tables,
            "Viste": views,
            "CSV": csv_hierarchy
        }

        # 6) Ricostruisci l'accordion principale
        objects_accordion.clear()
        checkbox_map.clear()
        for top_label, schema_dict in hierarchy.items():
            sub_acc = pn.Accordion()
            for schema_name, obj_names in schema_dict.items():
                cbg = pn.widgets.CheckBoxGroup(options=obj_names, inline=False)
                checkbox_map[(top_label, schema_name)] = cbg
                sub_acc.append((schema_name, cbg))
            objects_accordion.append((top_label, sub_acc))

    except Exception as e:
        output_area.visible = True
        output_area.object = f"**Eccezione**: {e}"

show_tables_button.on_click(show_objects)

# -----------------------------------------------------------------------------
# Funzione per confermare la selezione e recuperare i metadati delle tabelle
# -----------------------------------------------------------------------------
metadata_output = pn.pane.Markdown()
metadata_cache = {}  # Cache globale dei metadati
selected_tables = set()

def confirm_selection(event):
    """
    Legge le tabelle selezionate dall'accordion e le separa in due gruppi:
      - db_selected: tabelle/visti del database
      - csv_selected: file CSV (con schema "csv")
    Per le tabelle del DB, richiede i metadati tramite /get_table_metadata;
    per i CSV, chiama /get_csv_metadata (usando il nome del file senza estensione).
    Infine, aggiorna le opzioni del Relationship Builder con l'insieme dei metadati.
    """
    # Se non sono presenti oggetti nell'accordion, mostra un messaggio
    if len(checkbox_map.items()) == 0:
        metadata_output.object = (
            "**Nessun oggetto presente a backend**<br>"
            "Collega e carica oggetti dal database, o carica file CSV"
        )
        return

    global selected_tables, metadata_cache
    selected_tables = set()  # Inizializzo come set per evitare duplicati
    db_selected = []         # Per le tabelle del DB
    csv_selected = []        # Per i file CSV

    # Itera sui widget dell'accordion per raccogliere le selezioni
    for (top_label, schema_name), cbg in checkbox_map.items():
        if cbg.value:
            for t in cbg.value:
                full_name = f"{schema_name}.{t}"
                selected_tables.add(full_name)
                # Se lo schema è "csv" (case insensitive), è un file CSV
                if schema_name == "File CSV":
                    csv_selected.append(full_name)
                else:
                    db_selected.append(full_name)
    selected_tables = list(selected_tables)
    print("Oggetti selezionati:", selected_tables)

    if not selected_tables:
        metadata_output.object = "**Nessuna tabella selezionata.**"
        return

    # Inizializza la cache dei metadati
    metadata_cache = {}

    # 1) Richiede i metadati per le tabelle del database
    if db_selected:
        try:
            payload = {"tables": db_selected}
            resp = requests.post(f"{BACKEND_URL}/get_table_metadata", json=payload)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    metadata_cache.update(data.get("metadata", {}))
                else:
                    metadata_output.object = f"**Errore**: {data.get('details')}"
                    return
            else:
                metadata_output.object = f"**Errore HTTP**: {resp.text}"
                return
        except Exception as e:
            metadata_output.object = f"**Eccezione**: {e}"
            return

    # 2) Per ogni CSV, chiama l'endpoint /get_csv_metadata usando il nome del file (senza estensione)
    if csv_selected:
        for full_name in csv_selected:
            try:
                # full_name è del tipo "csv.nomefile"; estrai il nome
                _, csv_name = full_name.split(".", 1)
                csv_resp = requests.get(f"{BACKEND_URL}/get_csv_metadata?filename={csv_name}")
                if csv_resp.status_code == 200:
                    csv_data = csv_resp.json()
                    if csv_data.get("status") == "ok":
                        metadata_cache[full_name] = csv_data.get("metadata")
                    else:
                        # Se non trovi i metadati, imposta un default
                        metadata_cache[full_name] = [{
                            "column_name": "N/D",
                            "data_type": "object",
                            "max_length": None
                        }]
                else:
                    metadata_cache[full_name] = [{
                        "column_name": "N/D",
                        "data_type": "object",
                        "max_length": None
                    }]
            except Exception as e:
                metadata_cache[full_name] = [{
                    "column_name": "N/D",
                    "data_type": "object",
                    "max_length": None
                }]

    # 3) Aggiorna le select del Relationship Builder con le chiavi di metadata_cache
    table_options = list(metadata_cache.keys())
    table1_select.options = table_options
    table2_select.options = table_options
    metadata_output.object = "Metadati recuperati con successo."

# Pulsante per confermare la selezione degli oggetti del DB
confirm_objects_button = pn.widgets.Button(name="Conferma selezione", button_type="success")
confirm_objects_button.on_click(confirm_selection)


# =============================================================================
# Sezione 4: Relationship Builder (Costruzione del modello relazionale)
# =============================================================================
# Widgets per selezionare tabelle, colonne e il tipo di relazione
table1_select = pn.widgets.Select(name="Tabella 1", options=[])
column1_select = pn.widgets.Select(name="Colonna 1")
table2_select = pn.widgets.Select(name="Tabella 2", options=[])
column2_select = pn.widgets.Select(name="Colonna 2")
relationship_type_select = pn.widgets.Select(
    name="Tipo Relazione",
    options=["1:1", "1:N", "N:1", "N:N"],
    value="1:N"
)
add_relationship_button = pn.widgets.Button(name="Aggiungi Relazione", button_type="primary")
conferma_relazioni_button = pn.widgets.Button(name="Conferma Relazioni", button_type="success", visible=False)
conferma_output = pn.pane.Markdown("", visible=False)
relationship_rows = pn.Column(pn.pane.Markdown("#### Relazioni definite"), visible=False)

# Funzioni per aggiornare le colonne in base alla tabella selezionata
def update_columns1(event):
    """
    Aggiorna le opzioni per 'column1_select' in base ai metadati della tabella selezionata in 'table1_select'.
    """
    selected_table = table1_select.value
    if selected_table in metadata_cache:
        col_map = {}
        for col in metadata_cache[selected_table]:
            col_name = col["column_name"]
            dt = col["data_type"]
            ml = col["max_length"]
            if ml is not None and dt.lower() in ("varchar", "char", "nvarchar", "nchar"):
                label = f"{col_name} ({dt}({ml}))"
            elif ml is not None and dt.lower() in ("decimal", "numeric"):
                label = f"{col_name} ({dt}({ml}))"
            else:
                label = f"{col_name} ({dt})"
            col_map[label] = col_name
        column1_select.options = col_map
    else:
        column1_select.options = {}

def update_columns2(event):
    """
    Aggiorna le opzioni per 'column2_select' in base ai metadati della tabella selezionata in 'table2_select'.
    """
    selected_table = table2_select.value
    if selected_table in metadata_cache:
        col_map = {}
        for col in metadata_cache[selected_table]:
            col_name = col["column_name"]
            dt = col["data_type"]
            ml = col["max_length"]
            if ml is not None and dt.lower() in ("varchar", "char", "nvarchar", "nchar"):
                label = f"{col_name} ({dt}({ml}))"
            elif ml is not None and dt.lower() in ("decimal", "numeric"):
                label = f"{col_name} ({dt}({ml}))"
            else:
                label = f"{col_name} ({dt})"
            col_map[label] = col_name
        column2_select.options = col_map
    else:
        column2_select.options = {}

table1_select.param.watch(update_columns1, 'value')
table2_select.param.watch(update_columns2, 'value')

def create_relationship_row(rel):
    """
    Crea una riga (Row) che rappresenta una relazione definita, con un pulsante per rimuoverla.
    """
    label = f"{rel['table1']}.{rel['column1']} **{rel['type']}** {rel['table2']}.{rel['column2']}"
    label_md = pn.pane.Markdown(label, width_policy="max")
    remove_button = pn.widgets.Button(name="X", button_type="danger", width=40)
    row = pn.Row(label_md, remove_button, sizing_mode="stretch_width")

    def remove_rel(event):
        relationship_list.remove(rel)
        relationship_rows.remove(row)

    remove_button.on_click(remove_rel)
    return row

relationship_list = []  # Lista globale di relazioni

def add_relationship(event):
    """
    Aggiunge una nuova relazione alla lista e crea la relativa riga grafica.
    Se i campi richiesti non sono stati selezionati, notifica l'utente.
    """
    global selected_tables
    if not selected_tables:
        conferma_output.visible = True
        conferma_output.object = "**Nessuna tabella selezionata**"
        return
    relationship_rows.visible = True
    if table1_select.value and column1_select.value and table2_select.value and column2_select.value:
        rel = {
            "id": str(uuid.uuid4()),
            "table1": table1_select.value,
            "column1": column1_select.value,
            "table2": table2_select.value,
            "column2": column2_select.value,
            "type": relationship_type_select.value
        }
        relationship_list.append(rel)
        conferma_relazioni_button.visible = True
        conferma_output.visible = True
        row = create_relationship_row(rel)
        relationship_rows.append(row)
    else:
        conferma_output.visible = True
        relationship_rows.insert(1, pn.pane.Markdown(
            "**Attenzione:** Seleziona le tabelle, le colonne e il tipo di relazione!",
            style={'color': 'red'}
        ))

add_relationship_button.on_click(add_relationship)

def confirm_relationships(event):
    """
    Invia al backend la lista delle relazioni definite.
    Il payload è una lista di dizionari contenenti i dettagli della relazione.
    """
    if not relationship_list:
        conferma_output.object = "**Nessuna relazione da confermare.**"
        return
    
    payload = []
    for rel in relationship_list:
        payload.append({
            "table1": rel["table1"],
            "column1": rel["column1"],
            "table2": rel["table2"],
            "column2": rel["column2"],
            "type": rel["type"]
        })
    
    try:
        resp = requests.post(f"{BACKEND_URL}/store_relationships", json=payload)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "ok":
                conferma_output.object = (
                    f"**Relazioni memorizzate con successo**. "
                    f"{data.get('count', 0)} relazioni salvate. <br>"
                    "Adesso puoi procedere con l'interrogazione del modello di AI nella sezione corrispondente"
                )
            else:
                conferma_output.object = f"**Errore**: {data.get('message')}"
        else:
            conferma_output.object = f"**Errore HTTP**: {resp.text}"
    except Exception as e:
        conferma_output.object = f"**Eccezione**: {e}"

conferma_relazioni_button.on_click(confirm_relationships)

relationship_card = pn.layout.Card(
    pn.Row(table1_select, column1_select),
    pn.Row(table2_select, column2_select),
    relationship_type_select,
    add_relationship_button,
    relationship_rows,
    pn.Spacer(height=10),
    conferma_relazioni_button,
    conferma_output,
    title="Crea Relazioni",
    collapsible=True,
    visible=True,  
    header_color="#0d6efd",
    margin=(10, 10, 10, 10)
)


# =============================================================================
# Sezione 4.5: Visualizzazione modello dati
# =============================================================================
pn.extension()

# Pane per mostrare il diagramma
diagram_pane = pn.pane.SVG(
    link_url=f"{BACKEND_URL}/model_diagram",  # URL dell'endpoint
    embed=False,
    sizing_mode="stretch_width",
    height=600  # altezza a piacere
)

# diagram_pane=pn.Column()

visualizza_button = pn.widgets.Button(name="Visualizza Modello Dati", button_type="primary")

def on_visualizza_click(event):
    # Aggiorna la sorgente dell'immagine (per forzare un refresh)
    diagram_pane.object = f"{BACKEND_URL}/model_diagram?ts={time.time()}"
    # Dove time.time() serve a evitare caching

visualizza_button.on_click(on_visualizza_click)

# Oppure puoi semplicemente mostrare diagram_pane in una card
visualizza_card = pn.Column(
    visualizza_button,
    diagram_pane
)


# =============================================================================
# Sezione 5: Chat con LLM per generare query SQL
# =============================================================================
chat_input = pn.widgets.TextInput(name="La tua domanda in linguaggio naturale:")
chat_button = pn.widgets.Button(name="Invia", button_type="primary")
chat_output = pn.Column()
chat_history = []

def update_chat_history():
    """
    Aggiorna la visualizzazione della cronologia della chat.
    Per ogni scambio, visualizza la domanda, la query SQL generata e il risultato.
    """
    chat_output.clear()
    chat_output.append(pn.pane.Markdown("### Chat History"))
    for item in chat_history:
        block = pn.Column(
            pn.pane.Markdown(f"**Utente:** {item['user']}"),
            pn.pane.Markdown(f"**Query SQL generata:** {item['sql']}"),
            pn.pane.Markdown("**Risultato Query:**")
        )
        resp_data = item['response']
        if "error" in resp_data:
            block.append(pn.pane.Markdown(f"**Errore:** {resp_data['error']}"))
        else:
            columns = resp_data.get("columns", [])
            rows = resp_data.get("rows", [])
            if columns and rows:
                df = pd.DataFrame(rows, columns=columns)
                block.append(pn.pane.DataFrame(df, width=400))
            else:
                block.append(pn.pane.Markdown(str(resp_data)))
        block.append(pn.pane.Markdown("---"))
        chat_output.append(block)

def send_chat(event):
    """
    Invia la domanda dell'utente al backend per generare una query SQL e visualizza
    il risultato nella cronologia della chat.
    """
    question = chat_input.value.strip()
    if not question:
        chat_output.append(pn.pane.Markdown("**Attenzione:** Inserisci una domanda valida."))
        return
    payload = {"message": question}
    try:
        resp = requests.post(f"{BACKEND_URL}/chat", json=payload)
        if resp.status_code == 200:
            data = resp.json()
            generated_sql = data.get("sql_generated", "")
            results = data.get("results", {})
            chat_history.append({
                "user": question,
                "sql": generated_sql,
                "response": results
            })
            update_chat_history()
        else:
            chat_output.append(pn.pane.Markdown(f"**Errore nella chiamata:** {resp.text}"))
    except Exception as e:
        chat_output.append(pn.pane.Markdown(f"**Errore nella chiamata:** {e}"))

chat_button.on_click(send_chat)

chat_card = pn.layout.Card(
    pn.pane.Markdown("## Chat con LLM per generare query SQL"),
    chat_input,
    chat_button,
    chat_output,
    title="Chat LLM",
    collapsible=True,
    header_color="#6610f2",
    margin=(10, 10, 10, 10)
)


# =============================================================================
# Sezione 6: Navigazione e Template (Sidebar e Main Area)
# =============================================================================


# Definizione delle "pagine" per la navigazione
main_area = pn.Column()

def show_page(page):
    """Sostituisce il contenuto di main_area con la pagina scelta."""
    main_area.objects = [page]

# PULSANTI PER LA SEZIONE "Caricamento CSV" con sottopulsanti
csv_main_button = pn.widgets.Button(name="Caricamento CSV", button_type="primary")
carica_file_button = pn.widgets.Button(name="Carica file", button_type="light")
carica_file_button.on_click(lambda event: show_page(upload_card))
visualizza_file_button = pn.widgets.Button(name="Visualizza file caricati", button_type="light")
visualizza_file_button.on_click(lambda event: show_page(visualizza_file_card))

# Container per i sottopulsanti (inizialmente nascosto)
csv_subitems = pn.Column(carica_file_button, visualizza_file_button, visible=False)
def toggle_csv_subitems(event):
    csv_subitems.visible = not csv_subitems.visible
csv_main_button.on_click(toggle_csv_subitems)


# PULSANTI PER LA SEZIONE "Modellazione Dati" con sottopulsanti
data_model_button = pn.widgets.Button(name="Modellazione Dati", button_type="primary")
tables_selection_button = pn.widgets.Button(name="Seleziona Tabelle", button_type="light")
tables_selection_button.on_click(lambda event: show_page(data_model_card))
relationship_builder_button = pn.widgets.Button(name="Relazioni Del Modello", button_type="light")
relationship_builder_button.on_click(lambda event: show_page(relationship_card))
model_view_button = pn.widgets.Button(name="Visualizza Modello Dati", button_type="light")
model_view_button.on_click(lambda event: show_page(visualizza_card))

# Container per i sottopulsanti (modellazione dati)
model_subitems = pn.Column(tables_selection_button, relationship_builder_button,model_view_button, visible=False)
def toggle_model_subitems(event):
    model_subitems.visible = not model_subitems.visible
data_model_button.on_click(toggle_model_subitems)

# Pagina per la configurazione DB e modello dati (includendo anche il Relationship Builder)
page_db = pn.Column(db_card := pn.layout.Card(
    db_server,
    db_database,
    db_username,
    db_password,
    db_button,
    db_output,
    title="Configurazione Database",
    collapsible=True,
    header_color="#198754",
    margin=(10, 10, 10, 10)
))

# Pagina per la configurazione modello dati 
data_model_card = pn.Column(db_card := pn.layout.Card(    
    show_tables_button,
    objects_accordion,
    confirm_objects_button,
    output_area,
    metadata_output,
    title="Includi tabelle nel modello dati",
    collapsible=True,
    header_color="#198754",
    margin=(10, 10, 10, 10)
))

# Altri pulsanti singoli per navigare nelle altre sezioni
db_config_button = pn.widgets.Button(name="Configurazione Database", button_type="primary")
db_config_button.on_click(lambda event: show_page(page_db))
chat_nav_button = pn.widgets.Button(name="Chat LLM", button_type="primary")
chat_nav_button.on_click(lambda event: show_page(chat_card))

# Creazione del template FastListTemplate
template = pn.template.FastListTemplate(
    title="GeneraBI - MVP Chat con LLM e Query SQL",
    main=[main_area],
    accent_base_color="#0d6efd",
    header_background="#0d6efd",
    header_color="white",
)

# Aggiunta degli elementi alla sidebar
template.sidebar.append(csv_main_button)
template.sidebar.append(csv_subitems)
template.sidebar.append(db_config_button)
template.sidebar.append(data_model_button)
template.sidebar.append(model_subitems)
template.sidebar.append(chat_nav_button)
# Rendi servibile il template
template.servable()
