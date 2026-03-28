import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# ==========================================
# CONFIGURACIÓN BIGQUERY
# ==========================================
PROJECT_ID = "sistema-consolidado-registro"
DATASET_ID = "registros"
TABLE_ID = "colaboradores"
TABLE_FULL_NAME = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Ruta del archivo JSON
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

def render_registro():
    if not os.path.exists(CREDENTIALS_PATH) and "gcp_service_account" not in st.secrets:
        st.error("⚠️ No se encontró el archivo de credenciales. Si estás en local necesitas el `credenciales.json`. Si estás en la nube necesitas configurar `st.secrets`.")
        st.stop()

    st.header("📝 Registro de Asistentes a Capacitación")
    st.write("Registra los datos de la capacitación y añade a los participantes. Al escribir un DNI, sus datos se autocompletarán con la información del consolidado maestro alojado en BigQuery.")

    # 1. Datos de la Capacitación
    st.subheader("Datos de la Capacitación:")
    col_cap1, col_cap2 = st.columns(2)
    with col_cap1:
        st.text_input("Nombre de la Capacitación:", key="cap_nombre")
        st.text_input("Tienda:", key="cap_tienda")
        st.number_input("N° Hora:", min_value=1.0, step=0.5, key="cap_horas")
    with col_cap2:
        st.date_input("Fecha:", key="cap_fecha")
        st.selectbox("Modalidad:", ["Presencial", "Virtual"], key="cap_modalidad")
        st.file_uploader("Adjuntar el archivo", key="cap_archivo")
        
    # 2. Datos del Capacitador
    st.subheader("Datos del Capacitador:")

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.text_input("Tipo:", key="cap_tipo")
        st.text_input("DNI:", key="cap_dni")
        st.text_input("Apellidos y Nombres:", key="cap_nombres")
    with col_c2:
        st.text_input("Puesto:", key="cap_puesto")
        st.text_input("Área / Empresa:", key="cap_area")

    st.divider()

    # 3. Lista de Asistentes con Autocompletado Automático
    st.subheader("Lista de Asistentes:")

    # Inicializar la tabla en session_state si no existe
    if "df_asistentes" not in st.session_state:
        st.session_state["df_asistentes"] = pd.DataFrame(columns=[
            "DNI", "Código Ofisis", "Apellidos y Nombres", "Cargo", "Área", "Tienda", "Género", "Tipo de contrato", "Edad"
        ])
        # Agregar filas vacías de inicio
        for i in range(5):
            st.session_state["df_asistentes"].loc[i] = ["", "", "", "", "", "", "", "", ""]
            
    # Mostrar el editor de datos interactivo
    df_asistentes_editado = st.data_editor(
        st.session_state["df_asistentes"],
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        key="editor_asistentes" # Genera un widget state
    )

    # Búsqueda directa en BigQuery asumiendo nombres de campos permanentes e indexados
    necesita_actualizar = False
    client = get_bq_client()

    # ===== Lógica OPTIMIZADA para Autocompletado Masivo de Asistentes =====
    dnis_pendientes = []
    indices_pendientes = []
    
    for i, idx in enumerate(df_asistentes_editado.index):
        dni_ingresado = str(df_asistentes_editado.at[idx, "DNI"]).strip()
        
        # Solo procesar si el usuario introdujo un DNI
        if dni_ingresado and dni_ingresado not in ["", "nan", "None"]:
            nombres_actual = str(df_asistentes_editado.at[idx, "Apellidos y Nombres"]).strip()
            
            # Solo buscar si la celda de nombres está vacía o tiene un error temporal
            if not nombres_actual or nombres_actual in ["Error BQ", "No se encontró en consolidador BQ"]:
                dni_limpio = dni_ingresado.replace('.0', '')
                dnis_pendientes.append(dni_limpio)
                indices_pendientes.append((idx, dni_limpio))

    # Si encontramos DNIs que buscar, hacemos UNA SOLA consulta a la nube (ahorra mucho tiempo)
    if dnis_pendientes and client:
        # Poner comillas simples a cada DNI para la consulta SQL
        dnis_sql = ", ".join([f"'{d}'" for d in set(dnis_pendientes)])
        
        query = f"""
            SELECT * 
            FROM `{TABLE_FULL_NAME}`
            WHERE numero_de_documento_de_identidad_principal IN ({dnis_sql})
        """
        try:
            df_bq = client.query(query).to_dataframe()
            
            # Crear diccionario en memoria para acceso ultrarrápido O(1) local
            bq_dict = {}
            if not df_bq.empty:
                df_bq['numero_de_documento_de_identidad_principal'] = df_bq['numero_de_documento_de_identidad_principal'].astype(str)
                bq_dict = df_bq.set_index('numero_de_documento_de_identidad_principal').to_dict('index')
            
            # Mapear los resultados a las celdas directamente
            for idx, dni in indices_pendientes:
                if dni in bq_dict:
                    match = bq_dict[dni]
                    
                    df_asistentes_editado.at[idx, "Código Ofisis"] = str(match.get("id_ofiplan", ""))
                    df_asistentes_editado.at[idx, "Apellidos y Nombres"] = str(match.get("nombre", ""))
                    
                    # === CÁLCULO DE EDAD ===
                    fecha_nac = match.get("fecha_de_nacimiento_de_persona")
                    edad_str = ""
                    if pd.notna(fecha_nac) and str(fecha_nac).strip() != "" and str(fecha_nac).lower() != "nan":
                        try:
                            dt_nac = pd.to_datetime(fecha_nac, errors='coerce')
                            if pd.notna(dt_nac):
                                hoy = pd.Timestamp.now()
                                edad = hoy.year - dt_nac.year - ((hoy.month, hoy.day) < (dt_nac.month, dt_nac.day))
                                edad_str = str(edad)
                        except Exception:
                            pass
                            
                    df_asistentes_editado.at[idx, "Cargo"] = str(match.get("posicion_nombre", ""))
                    df_asistentes_editado.at[idx, "Área"] = str(match.get("nombre_del_departamento", ""))
                    df_asistentes_editado.at[idx, "Tienda"] = str(match.get("nombre_de_ubicacion", ""))
                    df_asistentes_editado.at[idx, "Género"] = str(match.get("genero_de_persona", ""))
                    df_asistentes_editado.at[idx, "Tipo de contrato"] = str(match.get("tipo_de_contrato", match.get("contrato", "")))
                    df_asistentes_editado.at[idx, "Edad"] = edad_str
                    
                    # Limpieza de nulos ('nan' -> espacio vacío)
                    for col in ["Código Ofisis", "Apellidos y Nombres", "Cargo", "Área", "Tienda", "Género", "Tipo de contrato", "Edad"]:
                        if df_asistentes_editado.at[idx, col] == "nan":
                            df_asistentes_editado.at[idx, col] = ""
                else:
                    df_asistentes_editado.at[idx, "Apellidos y Nombres"] = "No se encontró en consolidador BQ"
                    
                necesita_actualizar = True
                
        except Exception as e:
            # Si la consulta única falla, marcamos todos con error
            for idx, dni in indices_pendientes:
                df_asistentes_editado.at[idx, "Apellidos y Nombres"] = "Error BQ"
            necesita_actualizar = True

    # Si hubo cambios por autocompletar, guardar en session state y reiniciar para que la pantalla se dibuje con datos llenos
    if necesita_actualizar:
        st.session_state["df_asistentes"] = df_asistentes_editado
        st.rerun()
    else:
        # Siempre mantener sincronizado el session state con lo que el usuario tipea a mano
        st.session_state["df_asistentes"] = df_asistentes_editado
        
    st.write("")
    if st.button("Guardar Registro de Capacitación", type="primary"):
        df_validos = df_asistentes_editado[df_asistentes_editado["DNI"].astype(str).str.strip() != ""]
        if not df_validos.empty:
            st.success(f"¡Se han registrado {len(df_validos)} asistentes correctamente!")
            st.balloons()
        else:
            st.warning("No has ingresado ningún DNI válido.")

# Si se ejecuta este archivo individualmente (fuera del consolidador), mostrar el formulario
if __name__ == "__main__":
    st.set_page_config(page_title="Registro de Asistentes", page_icon="📝", layout="wide")
    render_registro()
