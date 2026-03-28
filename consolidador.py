import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import re
from io import BytesIO
import registro_asistentes

# Configuración de la página
st.set_page_config(page_title="Consolidador de Colaboradores", page_icon="👥", layout="wide")

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
            # En la nube de Streamlit leerá directamente de los Secrets
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        except Exception as e:
            st.error(f"Error en Nube: {e}")
            return None
    else:
        return None

def cargar_maestro():
    """Descarga el consolidado desde BigQuery."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame() # Si no hay cliente, retorna vacío
        
    try:
        query = f"SELECT * FROM `{TABLE_FULL_NAME}`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        return pd.DataFrame()

def guardar_maestro(df):
    """Sube el DataFrame sobrescribiendo la tabla en BigQuery."""
    client = get_bq_client()
    if not client:
        st.error("No se pudo conectar a BigQuery. Verifica el archivo credenciales.json.")
        return False
        
    try:
        # 1. Asegurar que el Dataset existe por si el usuario no lo creó manualmente
        dataset_id_full = f"{PROJECT_ID}.{DATASET_ID}"
        try:
            client.get_dataset(dataset_id_full)
        except Exception:
            # Si lanza excepción es porque no existe (404), intentamos crearlo
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = "US" 
            client.create_dataset(dataset, timeout=30)

        # 2. Configuramos para reemplazar la tabla
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True, # Crea las columnas automáticamente si no existen
        )
        
        job = client.load_table_from_dataframe(df, TABLE_FULL_NAME, job_config=job_config)
        job.result() # Esperar a que termine la subida
        return True
    except Exception as e:
        st.error(f"Error al guardar en BigQuery: {e}")
        return False


def sanitizar_dataframe(df_sucio):
    """Limpia los nombres de las columnas y los tipos de datos para que BigQuery no falle."""
    import unicodedata
    cols_limpias = []
    for col in df_sucio.columns:
        # Remover tildes y caracteres especiales, dejar solo ASCII
        c = unicodedata.normalize('NFKD', str(col)).encode('ascii', 'ignore').decode('ascii')
        c = re.sub(r'[^\w]', '_', c).strip('_').lower()
        c = re.sub(r'_+', '_', c)
        if not c: c = 'col'
        # BigQuery exige que los nombres de columna no empiecen por número
        if c[0].isdigit(): c = 'c_' + c
        cols_limpias.append(c)
        
    # Evitar nombres de columnas duplicados
    cols_finales = []
    for c in cols_limpias:
        cc = c
        i = 1
        while cc in cols_finales:
            cc = f"{c}_{i}"
            i += 1
        cols_finales.append(cc)
        
    df_sucio.columns = cols_finales
    
    # Palabras clave para columnas que DEBEN ser texto (IDs, DNIs, Códigos, etc.)
    palabras_texto = ['dni', 'documento', 'identidad', 'id_ofiplan', 'ofisis', 'codigo', 'cod_']
    
    # Convertir todas las columnas con tipos mezclados o que sean identificadores a texto (string)
    for c in df_sucio.columns:
        es_columna_texto = any(p in c.lower() for p in palabras_texto)
        
        if es_columna_texto or df_sucio[c].dtype == 'object':
            df_sucio[c] = df_sucio[c].astype(str)
            # Eliminar .0 al final si pandas lo leyó como float
            df_sucio[c] = df_sucio[c].str.replace(r'\.0$', '', regex=True)
            # Reemplazar 'nan' y 'None' reales por cadenas vacías
            df_sucio[c] = df_sucio[c].replace(['nan', 'None', '<NA>', 'NaN'], '')
            
    return df_sucio

def sugerir_columna_clave(columnas):
    """Auto-detecta la mejor columna para usar como identificador único."""
    palabras_fuertes = ['dni', 'documento', 'identidad', 'cedula', 'rut']
    palabras_medias = ['id_', '_id', 'codigo', 'cod_']
    
    columnas_lower = [str(c).lower() for c in columnas]
    
    # 1. DNIs y Documentos
    for i, col in enumerate(columnas_lower):
        if any(p in col for p in palabras_fuertes):
            return i
            
    # 2. Códigos e IDs numéricos
    for i, col in enumerate(columnas_lower):
        if any(p in col for p in palabras_medias):
            return i
            
    # 3. Nombre (Petición del usuario como respaldo, evitando "nombre_de_unidad" o similares)
    for i, col in enumerate(columnas_lower):
        if 'nombre' in col and 'unidad' not in col and 'cargo' not in col and 'posici' not in col:
            return i
            
    return 0 # Por defecto la primera si no halla similitud


# Inicializar maestro
if "df_maestro" not in st.session_state:
    st.session_state["df_maestro"] = cargar_maestro()

st.title("👥 Sistema de Gestión de Colaboradores")
st.write("Mantén un registro centralizado y actualizado alojado en Google BigQuery.")

if not os.path.exists(CREDENTIALS_PATH) and "gcp_service_account" not in st.secrets:
    st.error("⚠️ No se encontró el archivo de credenciales. Si estás en local necesitas el `credenciales.json`. Si estás en la nube necesitas configurar `st.secrets`.")
    st.stop()

# Crear las pestañas principales
tab_visor, tab_carga, tab_registro = st.tabs([
    "📊 Ver Consolidado Maestro", 
    "🔄 Cargar Nuevos Archivos",
    "📝 Registro de Asistentes"
])

# ==========================================
# PESTAÑA 1: VISOR DEL MAESTRO
# ==========================================
with tab_visor:
    st.header("Base de Datos en BigQuery")
    
    df_actual = st.session_state["df_maestro"]
    
    if df_actual.empty:
        st.info("ℹ️ La tabla de BigQuery está vacía o no existe. Ve a la pestaña 'Cargar Nuevos Archivos' para inicializarla.")
    else:
        st.metric(label="Total de Colaboradores Registrados", value=len(df_actual))
        st.dataframe(df_actual, use_container_width=True)
        st.divider()
        st.subheader("📥 Descargar Base de Datos Completa")
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_actual.to_excel(writer, index=False, sheet_name='Maestro')
        excel_data = output.getvalue()
        
        st.download_button(
            label="Descargar Consolidado (Excel)",
            data=excel_data,
            file_name="consolidado_bigquery.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )

# ==========================================
# PESTAÑA 2: CARGA Y ACTUALIZACIÓN
# ==========================================
with tab_carga:
    st.header("Actualizar Consolidado")
    st.write("Sube un nuevo archivo para fusionarlo y guardarlo en la nube.")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        # Importante: Muchos reportes de Excel tienen un súper título en la fila 1 y los verdaderos encabezados (DNI, Nombre...) en otra fila
        fila_encabezado = st.number_input("¿En qué fila están los títulos (DNI, Nombres)?", min_value=1, value=1, step=1, help="Si tu Excel tiene un título gigante principal, ajusta este número a la fila donde verdaderamente empiezan las columnas de datos (Ej: 2 o 3).")
    
    with col2:
        archivo_nuevo = st.file_uploader("Sube el nuevo archivo a procesar", type=["csv", "xlsx", "xls"])
    
    if archivo_nuevo is not None:
        try:
            saltarse_filas = fila_encabezado - 1
            # Leemos todo como texto (dtype=str) para evitar que Pandas borre los ceros a la izquierda de los DNIs
            if archivo_nuevo.name.endswith('.csv'):
                df_nuevo = pd.read_csv(archivo_nuevo, skiprows=saltarse_filas, dtype=str)
            else:
                df_nuevo = pd.read_excel(archivo_nuevo, skiprows=saltarse_filas, dtype=str)
                
            # SANITIZACIÓN: Crucial para BigQuery y PyArrow
            df_nuevo = sanitizar_dataframe(df_nuevo)
                
            st.success(f"Archivo analizado correctamente. Tiene {len(df_nuevo)} registros a procesar.")
            
            with st.expander("Ver columnas detectadas (Si se ven raras, ajusta la fila de inicio arriba)"):
                st.dataframe(df_nuevo.head())
                
            df_maestro_actual = st.session_state["df_maestro"]
            
            st.divider()
            st.subheader("⚙️ Configuración de la Fusión")
            
            idx_nuevo = sugerir_columna_clave(df_nuevo.columns)
            columna_clave_nuevo = st.selectbox(
                "Identificador en el nuevo archivo subido (Ej. DNI):", 
                options=df_nuevo.columns,
                index=idx_nuevo
            )
            
            columna_clave_maestro = None
            if not df_maestro_actual.empty:
                idx_maestro = sugerir_columna_clave(df_maestro_actual.columns)
                columna_clave_maestro = st.selectbox(
                    "Identificador en la Base de Datos Histórica (Maestro):", 
                    options=df_maestro_actual.columns,
                    index=idx_maestro
                )
            
            if_can_merge = True
            if not df_maestro_actual.empty and not columna_clave_maestro:
                if_can_merge = False
                
            ya_procesado = st.session_state.get("archivo_guardado") == archivo_nuevo.name
            
            if ya_procesado:
                rn, ra, tot = st.session_state.get("resumen_guardado", (0, 0, 0))
                st.success("✅ Este archivo ya fue procesado y guardado permanentemente en BigQuery.")
                st.info(f"📊 Resumen del movimiento que se realizó:\n"
                        f"- Nuevos colaboradores registrados: {rn}\n"
                        f"- Colaboradores preexistentes (actualizados/omitidos): {ra}\n"
                        f"- Total en la nueva base histórica: {tot}")
            elif if_can_merge and st.button("🚀 Guardar Definitivamente en BigQuery", type="primary"):
                with st.spinner("Conectando con Google Cloud y procesando masivamente..."):
                    
                    if df_maestro_actual.empty:
                        df_final = df_nuevo.drop_duplicates(subset=[columna_clave_nuevo], keep='first')
                        registros_nuevos = len(df_final)
                        registros_actualizados = 0
                    else:
                        if columna_clave_nuevo != columna_clave_maestro:
                            df_temp = df_nuevo.copy()
                            # EVITAR BUG PANDAS: Si por accidente ya había una columna que se llama igual al maestro, la quitamos
                            if columna_clave_maestro in df_temp.columns:
                                df_temp = df_temp.drop(columns=[columna_clave_maestro])
                                
                            df_nuevo_renombrado = df_temp.rename(columns={columna_clave_nuevo: columna_clave_maestro})
                            clave_a_usar = columna_clave_maestro
                        else:
                            df_nuevo_renombrado = df_nuevo.copy()
                            clave_a_usar = columna_clave_maestro
                        
                        import numpy as np
                        
                        # === LÓGICA DE UNIFICACIÓN ESTRICTA ===
                        # Retenemos exactamente los 36 campos estructurales (y eliminamos la "basura" del archivo nuevo)
                        columnas_base = df_maestro_actual.columns
                        columnas_comunes = df_nuevo_renombrado.columns.intersection(columnas_base)
                        df_nuevo_filtrado = df_nuevo_renombrado[columnas_comunes]
                        
                        # 1. Preparar índices para el Upsert
                        df_m_idx = df_maestro_actual.set_index(clave_a_usar)
                        # Quitar duplicados internos y aislar un dato por persona
                        df_n_idx = df_nuevo_filtrado.drop_duplicates(subset=[clave_a_usar], keep='last').set_index(clave_a_usar)
                        
                        # 2. Las celdas en blanco o vacías deben tratarse como NaN
                        df_n_idx = df_n_idx.replace(r'^\s*$', np.nan, regex=True)
                        
                        # 3. Fusión: combinamos para actualizar la vieja data
                        df_final = df_n_idx.combine_first(df_m_idx).reset_index()
                        
                        # Obligamos a que queden SOLO los 36 campos base
                        df_final = df_final[columnas_base]
                        
                        total_antes = len(df_maestro_actual)
                        total_despues = len(df_final)
                        registros_nuevos = max(0, total_despues - total_antes)
                        registros_actualizados = len(df_nuevo) - registros_nuevos
                    
                    # SANITIZACIÓN FINAL POR SI ACASO
                    df_final = sanitizar_dataframe(df_final)

                    # Cargar a BQ
                    exito = guardar_maestro(df_final)
                    
                    if exito:
                        st.session_state["df_maestro"] = df_final
                        st.session_state["archivo_guardado"] = archivo_nuevo.name
                        st.session_state["resumen_guardado"] = (registros_nuevos, registros_actualizados, len(df_final))
                        st.rerun()

        except Exception as e:
            st.error(f"Ocurrió un error general: {e}")

    st.divider()
    st.subheader("⚠️ Zona de Administrador")
    with st.expander("Borrar Base de Datos Maestra (Reset)"):
        st.warning("Si borras la base de datos, perderás el historial actual en la nube. Útil si deseas subir una base limpia o corregir la estructura desde cero.")
        confirmacion = st.checkbox("Estoy completamente seguro de borrar todos los datos en BigQuery.")
        if confirmacion:
            if st.button("🗑️ Eliminar Consolidado Permanentemente", type="primary"):
                client = get_bq_client()
                if client:
                    try:
                        client.delete_table(TABLE_FULL_NAME, not_found_ok=True)
                        st.session_state["df_maestro"] = pd.DataFrame()
                        st.success("¡La base ha sido eliminada con éxito! Sube tu archivo de 36 campos para crear una tabla nueva.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al intentar eliminar la tabla: {e}")

# ==========================================
# PESTAÑA 3: REGISTRO DE ASISTENTES
# ==========================================
with tab_registro:
    registro_asistentes.render_registro()


