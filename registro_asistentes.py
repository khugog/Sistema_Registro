import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import uuid
import datetime
import math

# ==========================================
# CONFIGURACIÓN BIGQUERY
# ==========================================
PROJECT_ID = "sistema-consolidado-registro"
DATASET_ID = "registros"
TABLE_ID = "colaboradores"
TABLE_FULL_NAME = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
TABLE_CAPACITACIONES = f"{PROJECT_ID}.{DATASET_ID}.datos_de_la_capacitacion"
TABLE_CAPACITADORES  = f"{PROJECT_ID}.{DATASET_ID}.datos_del_capacitador"
TABLE_ASISTENTES     = f"{PROJECT_ID}.{DATASET_ID}.lista_de_asistentes"

CREDENTIALS_PATH = "credenciales.json"

@st.cache_resource
def get_bq_client():
    """Inicializa el cliente de BigQuery (Compatible con Local y Streamlit Cloud)."""
    if os.path.exists(CREDENTIALS_PATH):
        try:
            credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            st.error(f"Error local: {e}")
            return None
    elif "gcp_service_account" in st.secrets:
        try:
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            st.error(f"Error en Nube: {e}")
            return None
    else:
        return None

@st.cache_data(ttl=600)
def buscar_dnis_en_bq(dnis_tuple):
    """Consulta BigQuery solo por los DNIs necesarios y cachea el resultado."""
    if not dnis_tuple: return pd.DataFrame()
    client = get_bq_client()
    if not client: return pd.DataFrame()
    
    try:
        # OBTENER EL NOMBRE REAL DE LAS COLUMNAS (Para evitar errores de 'columna no encontrada')
        table = client.get_table(TABLE_FULL_NAME)
        nombres_columnas = [field.name for field in table.schema]
        
        # Identificar cuál es la columna de DNI en esta tabla específica
        posibles_dni = ['numero_de_documento', 'dni', 'documento', 'identidad', 'cedula']
        col_dni_real = next((c for c in nombres_columnas if c.lower() in posibles_dni), None)
        
        if not col_dni_real:
            # Fallback: buscar cualquier columna que contenga la palabra
            col_dni_real = next((c for c in nombres_columnas if any(p in c.lower() for p in posibles_dni)), None)
        
        if not col_dni_real:
            return pd.DataFrame() # No se pudo identificar la columna de DNI
            
        dnis_query = ", ".join([f"'{d}'" for d in dnis_tuple])
        full_query = f"SELECT * FROM `{TABLE_FULL_NAME}` WHERE {col_dni_real} IN ({dnis_query})"
        
        return client.query(full_query).to_dataframe()
    except Exception as e:
        # Opcional: imprimir error para debug si fuera necesario
        return pd.DataFrame()

def guardar_en_bq(df, table_id):
    """Sube un DataFrame a BigQuery con disposición APPEND."""
    client = get_bq_client()
    if client is None: return False
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        return True
    except Exception as e:
        st.error(f"Error al guardar en {table_id}: {e}")
        return False

def render_registro():
    # --- 1. RADAR DE CIERRE (Se activa después de un rerun) ---
    if st.session_state.get("ejecutar_cierre_loader"):
        st.markdown('<div class="señal-finalizado"></div>', unsafe_allow_html=True)
        st.session_state["ejecutar_cierre_loader"] = False 

    # --- 2. Verificación de seguridad ---
    if not os.path.exists(CREDENTIALS_PATH) and "gcp_service_account" not in st.secrets:
        st.error("⚠️ No se encontró el archivo de credenciales.")
        st.stop()

    st.header("📝 Registro de Asistentes a Capacitación")

    # Gestión de mensajes
    if '_msg_exito' in st.session_state: 
        st.success(st.session_state.pop('_msg_exito'))
    if '_msg_error' in st.session_state: 
        st.error(st.session_state.pop('_msg_error'))
    if st.session_state.get('_mostrar_balloons', False): 
        st.balloons()
        st.session_state["_mostrar_balloons"] = False
    
    # Inputs de Cabecera
    st.subheader("Datos de la Capacitación:")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Nombre de la Capacitación:", key="cap_nombre")
        st.text_input("Tienda:", key="cap_tienda")
        st.number_input("N° Hora:", format="%.2f", step=1.0, value=1.00, key="cap_hora")
    with col2:
        st.date_input("Fecha:", key="cap_fecha")
        st.selectbox("Modalidad:", ["Presencial", "Virtual"], key="cap_modalidad")
        st.file_uploader("Adjuntar el archivo", key="cap_archivo")

    st.subheader("Datos del Capacitador:")
    col3, col4 = st.columns(2)
    with col3:
        st.text_input("Tipo:", key="cap_tipo")
        st.text_input("DNI:", key="cap_dni")
        st.text_input("Apellidos y Nombres:", key="cap_instructor_nombres")
    with col4:
        st.text_input("Puesto:", key="cap_puesto")
        st.text_input("Área / Empresa:", key="cap_area_empresa")
    st.divider()

    # --- 4. Gestión de Tabla ---
    columnas = ["DNI", "Código Ofisis", "Apellidos y Nombres", "Cargo",
                "Área", "Tienda", "Género", "Tipo de contrato", "Edad"]

    if "df_asistentes" not in st.session_state:
        df_init = pd.DataFrame("", index=range(30), columns=columnas)
        st.session_state["df_asistentes"] = df_init

    # Formulario
    with st.form("formulario_registro"):
        df_asistentes_editado = st.data_editor(
            st.session_state["df_asistentes"],
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="editor_asistentes"
        )
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            btn_validar = st.form_submit_button("🔍 Validar DNI", use_container_width=True)
        with col_btn2:
            btn_guardar = st.form_submit_button("💾 Guardar Registro de Capacitación", type="primary", use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # 5. LÓGICA DE PROCESAMIENTO
    # ═══════════════════════════════════════════════════════════════

    if btn_validar:
        df = df_asistentes_editado.copy().fillna("").replace("None", "")
        dnis_pendientes = []
        indices_pendientes = []
            
        for idx in df.index:
            dni_raw = str(df.at[idx, "DNI"]).strip()
            if dni_raw and dni_raw not in ["", "nan", "None"]:
                dni = dni_raw.replace('.0', '')
                dnis_pendientes.append(dni)
                indices_pendientes.append((idx, dni))

        if not dnis_pendientes:
            st.info("ℹ️ No hay DNIs nuevos para validar.")
        else:
            try:
                # CONSULTA TURBO
                df_res = buscar_dnis_en_bq(tuple(dnis_pendientes))
                bq_dict = {}
                
                if not df_res.empty:
                    # Detectar columna de DNI en resultados
                    col_idx = None
                    for c in df_res.columns:
                        if any(p in c.lower() for p in ['numero_de_documento', 'dni', 'documento', 'identidad']):
                            col_idx = c
                            break
                    if col_idx:
                        df_res[col_idx] = df_res[col_idx].astype(str).str.replace(r'\.0$', '', regex=True)
                        bq_dict = df_res.set_index(col_idx).to_dict('index')

                encontrados = 0
                for idx, dni in indices_pendientes:
                    if dni in bq_dict:
                        m = bq_dict[dni]
                        def buscar_valor(claves_posibles):
                            for k in claves_posibles:
                                if k in m and pd.notna(m[k]): return m[k]
                            return ""

                        df.at[idx, "Código Ofisis"] = str(buscar_valor(["id_ofiplan", "codigo_ofisis", "codigo", "ofisis"]))
                        df.at[idx, "Apellidos y Nombres"] = str(buscar_valor(["nombre", "nombres", "apellidos_y_nombres", "nombre_completo", "colaborador"]))
                        df.at[idx, "Cargo"] = str(buscar_valor(["posicion_nombre", "cargo", "puesto", "posicion", "ocupacion"]))
                        df.at[idx, "Área"] = str(buscar_valor(["nombre_del_departamento", "area", "departamento", "gerencia"]))
                        df.at[idx, "Tienda"] = str(buscar_valor(["nombre_de_ubicacion", "tienda", "ubicacion", "sede", "sucursal", "local"]))
                        df.at[idx, "Género"] = str(buscar_valor(["genero_de_persona", "genero", "sexo"]))
                        df.at[idx, "Tipo de contrato"] = str(buscar_valor(["tipo_de_contrato", "contrato"]))
                        
                        fnac = buscar_valor(["fecha_de_nacimiento_de_persona", "fecha_nacimiento", "nacimiento"])
                        if fnac:
                            try:
                                dt = pd.to_datetime(fnac)
                                df.at[idx, "Edad"] = str(pd.Timestamp.now().year - dt.year)
                            except: pass
                        encontrados += 1
                    else:
                        df.at[idx, "Apellidos y Nombres"] = "No encontrado"

                st.session_state["df_asistentes"] = df
                st.session_state["_msg_exito"] = f"✅ Validados {encontrados} colaboradores."
                st.session_state["ejecutar_cierre_loader"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    if btn_guardar:
        # 1. Validaciones básicas
        if not st.session_state.get("cap_nombre") or not st.session_state.get("cap_dni"):
            st.warning("⚠️ Por favor, completa el Nombre de la Capacitación y el DNI del Capacitador.")
            st.stop()

        # 2. Generar ID de Capacitación único
        id_cap = f"CAP-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:4]}"

        # 3. Preparar Datos de Capacitación
        df_cap = pd.DataFrame([{
            "ID_Capacitacion": id_cap,
            "Nombre_de_la_Capacitacion": st.session_state["cap_nombre"],
            "Fecha": str(st.session_state["cap_fecha"]),
            "N_Horas": float(st.session_state["cap_hora"]),
            "Modalidad": st.session_state["cap_modalidad"],
            "Tienda": st.session_state["cap_tienda"],
            "Nombre_Archivo_Adjunto": st.session_state["cap_archivo"].name if st.session_state["cap_archivo"] else "",
            "Fecha_Carga_Sistema": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }])

        # 4. Preparar Datos del Capacitador
        df_coach = pd.DataFrame([{
            "ID_Capacitacion": id_cap,
            "DNI": str(st.session_state["cap_dni"]),
            "Apellidos_y_Nombres": st.session_state["cap_instructor_nombres"],
            "Tipo_Capacitador": st.session_state["cap_tipo"],
            "Puesto": st.session_state["cap_puesto"],
            "Area_Empresa": st.session_state["cap_area_empresa"]
        }])

        # 5. Preparar Lista de Asistentes
        df_asist = df_asistentes_editado.copy().fillna("").replace("None", "")
        # Filtrar solo los que tienen DNI
        df_asist = df_asist[df_asist["DNI"].astype(str).str.strip() != ""]
        
        if df_asist.empty:
            st.warning("⚠️ No hay asistentes en la lista para guardar.")
            st.stop()
            
        # Renombrar columnas para que coincidan con BigQuery
        mapping_cols = {
            "DNI": "DNI",
            "Código Ofisis": "Codigo_Ofisis",
            "Apellidos y Nombres": "Apellidos_y_Nombres",
            "Cargo": "Cargo",
            "Área": "Area",
            "Tienda": "Tienda",
            "Género": "Genero",
            "Tipo de contrato": "Tipo_de_contrato",
            "Edad": "Edad"
        }
        df_asist = df_asist.rename(columns=mapping_cols)
        df_asist["ID_Capacitacion"] = id_cap
        
        # Asegurar que todas las columnas existan (por si acaso el editor cambió algo)
        for col in ["Codigo_Ofisis", "Apellidos_y_Nombres", "Cargo", "Area", "Tienda", "Genero", "Tipo_de_contrato", "Edad"]:
            if col not in df_asist.columns:
                df_asist[col] = ""

        # 6. Ejecutar Guardado
        with st.spinner("Guardando en BigQuery..."):
            exito1 = guardar_en_bq(df_cap, TABLE_CAPACITACIONES)
            exito2 = guardar_en_bq(df_coach, TABLE_CAPACITADORES)
            exito3 = guardar_en_bq(df_asist, TABLE_ASISTENTES)

            if exito1 and exito2 and exito3:
                st.session_state["_msg_exito"] = "✅ Registro completo guardado exitosamente en BigQuery."
                st.session_state["_mostrar_balloons"] = True
                st.session_state["ejecutar_cierre_loader"] = True
                
                # --- LIMPIEZA DE FORMULARIO PARA NUEVO REGISTRO ---
                # Reseteamos los valores directamente en el session_state
                st.session_state["cap_nombre"] = ""
                st.session_state["cap_tienda"] = ""
                st.session_state["cap_hora"] = 1.00
                st.session_state["cap_fecha"] = datetime.date.today()
                st.session_state["cap_modalidad"] = "Presencial"
                st.session_state["cap_tipo"] = ""
                st.session_state["cap_dni"] = ""
                st.session_state["cap_instructor_nombres"] = ""
                st.session_state["cap_puesto"] = ""
                st.session_state["cap_area_empresa"] = ""
                
                # Para el archivo y el estado del editor, usamos del para forzar el reset
                if "cap_archivo" in st.session_state: del st.session_state["cap_archivo"]
                if "editor_asistentes" in st.session_state: del st.session_state["editor_asistentes"]
                
                # Resetear la tabla a 30 filas vacías
                columnas_tabla = ["DNI", "Código Ofisis", "Apellidos y Nombres", "Cargo",
                                 "Área", "Tienda", "Género", "Tipo de contrato", "Edad"]
                st.session_state["df_asistentes"] = pd.DataFrame("", index=range(30), columns=columnas_tabla)
                
                st.rerun()
            else:
                st.error("❌ Hubo un error al guardar algunos datos. Por favor verifica la conexión.")

if __name__ == "__main__":
    st.set_page_config(page_title="Registro de Asistentes", page_icon="📝", layout="wide")
    render_registro()