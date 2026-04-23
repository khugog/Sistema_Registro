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

def generar_siguiente_id(client):
    try:
        query = f"SELECT MAX(ID_Capacitacion) as max_id FROM `{TABLE_CAPACITACIONES}`"
        df = client.query(query).to_dataframe()
        if not df.empty and pd.notna(df.iloc[0]['max_id']):
            max_id_str = str(df.iloc[0]['max_id']).strip()
            if max_id_str.startswith('A') and max_id_str[1:].isdigit():
                return f"A{int(max_id_str[1:]) + 1:09d}"
    except Exception:
        pass
    return "A000000001"


def render_registro():
    # --- 1. RADAR DE CIERRE (Se activa después de un rerun) ---
    if st.session_state.get("ejecutar_cierre_loader"):
        st.markdown('<div class="señal-finalizado"></div>', unsafe_allow_html=True)
        st.session_state["ejecutar_cierre_loader"] = False 

    # --- 2. Verificación de seguridad y limpieza de mensajes ---
    if not os.path.exists(CREDENTIALS_PATH) and "gcp_service_account" not in st.secrets:
        st.error("⚠️ No se encontró el archivo de credenciales.")
        st.stop()

    st.header("📝 Registro de Asistentes a Capacitación")

    # Gestión de mensajes
    if '_msg_exito' in st.session_state: 
        st.success(st.session_state.pop('_msg_exito'))
    if '_msg_error' in st.session_state: 
        st.error(st.session_state.pop('_msg_error'))
    if st.session_state.pop('_mostrar_balloons', False): 
        st.balloons()
    
    # 2. Inputs de Cabecera (Capacitación e Instructor)
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
        df = df_asistentes_editado.copy()
        df = df.fillna("")
        df = df.replace("None", "")
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
                # Usar referencia directa para velocidad instantánea
                df_bq = st.session_state.get("df_maestro", pd.DataFrame())
                bq_dict = {}
                if not df_bq.empty:
                    # Detectar dinámicamente la columna del DNI
                    col_dni = None
                    for c in df_bq.columns:
                        if any(p in c.lower() for p in ['numero_de_documento', 'dni', 'documento', 'identidad']):
                            col_dni = c
                            break
                    
                    if col_dni:
                        df_bq[col_dni] = df_bq[col_dni].astype(str).str.replace(r'\.0$', '', regex=True)
                        bq_dict = df_bq.set_index(col_dni).to_dict('index')

                encontrados = 0
                for idx, dni in indices_pendientes:
                    if dni in bq_dict:
                        m = bq_dict[dni]
                        
                        # Función auxiliar para buscar valores robustamente
                        def buscar_valor(claves_posibles):
                            for k in claves_posibles:
                                if k in m and pd.notna(m[k]):
                                    return m[k]
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
                        df.at[idx, "Apellidos y Nombres"] = "No se encontró en consolidador BQ"

                st.session_state["df_asistentes"] = df
                st.session_state["_msg_exito"] = f"✅ Se encontraron y validaron {encontrados} colaboradores."
                st.session_state["ejecutar_cierre_loader"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Error al validar: {e}")
    
    # Señal de seguridad por si no entra al try
    st.markdown('<div class="señal-finalizado"></div>', unsafe_allow_html=True)

    if btn_guardar:
        df_clean = df_asistentes_editado.copy().fillna("").replace("None", "")
        df_validos = df_clean[df_clean["DNI"].astype(str).str.strip() != ""]
            
        if df_validos.empty:
            st.warning("⚠️ Tabla vacía.")
        else:
            client = get_bq_client()
            if client:
                    try:
                        # (Tu lógica de guardado en BigQuery...)
                        
                        # PREPARACIÓN PARA EL CIERRE
                        st.session_state["_msg_exito"] = "✅ Guardado exitosamente."
                        st.session_state["_mostrar_balloons"] = True
                        st.session_state["ejecutar_cierre_loader"] = True # <--- CLAVE
                        
                        # Limpieza de tabla
                        df_reset = pd.DataFrame("", index=range(30), columns=columnas)
                        st.session_state["df_asistentes"] = df_reset
                        
                        st.rerun() 
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")
                        st.markdown('<div class="señal-finalizado"></div>', unsafe_allow_html=True)

# Ejecución principal (Solo útil si ejecutas este archivo directamente)
if __name__ == "__main__":
    st.set_page_config(page_title="Registro de Asistentes", page_icon="📝", layout="wide")
    render_registro()