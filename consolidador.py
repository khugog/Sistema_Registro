import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import re
from io import BytesIO
import registro_asistentes

# 1. Configuración de la página
st.set_page_config(page_title="Consolidador de Colaboradores", page_icon="👥", layout="wide")

# Ocultar cargadores nativos de Streamlit y evitar el oscurecimiento al escribir
st.markdown("""
<style>
div[data-testid="stStatusWidget"], .stSpinner, circle { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }

/* Evitar que la pantalla se oscurezca (dimming) al escribir en los inputs */
[data-stale="true"] {
    opacity: 1 !important;
    filter: none !important;
    transition: none !important;
}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. LÓGICA DE BIGQUERY (Toda la "inteligencia")
# ==========================================
PROJECT_ID = "sistema-consolidado-registro"
DATASET_ID = "registros"
TABLE_ID = "colaboradores"
TABLE_FULL_NAME = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
CREDENTIALS_PATH = "credenciales.json"

@st.cache_resource
def get_bq_client():
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
    return None

@st.cache_data(ttl=600)
def cargar_maestro():
    """Descarga el consolidado desde BigQuery y lo cachea por 10 minutos."""
    client = get_bq_client()
    if not client: return pd.DataFrame() 
    try:
        query = f"SELECT * FROM `{TABLE_FULL_NAME}`"
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame()

def guardar_maestro(df):
    client = get_bq_client()
    if not client: return False
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        job = client.load_table_from_dataframe(df, TABLE_FULL_NAME, job_config=job_config)
        job.result() 
        return True
    except Exception as e:
        st.error(f"Error en BigQuery: {e}")
        return False

def sanitizar_dataframe(df_sucio):
    import unicodedata
    cols_limpias = []
    for col in df_sucio.columns:
        c = unicodedata.normalize('NFKD', str(col)).encode('ascii', 'ignore').decode('ascii')
        c = re.sub(r'[^\w]', '_', c).strip('_').lower()
        if not c: c = 'col'
        if c[0].isdigit(): c = 'c_' + c
        cols_limpias.append(c)
    df_sucio.columns = cols_limpias
    for c in df_sucio.columns:
        if any(p in c.lower() for p in ['dni', 'documento', 'id', 'codigo']):
            df_sucio[c] = df_sucio[c].astype(str).str.replace(r'\.0$', '', regex=True).replace(['nan', 'None'], '')
    return df_sucio

if "df_maestro" not in st.session_state:
    st.session_state["df_maestro"] = cargar_maestro()

# ==========================================
# 3. DISEÑO Y NAVEGACIÓN (Original)
# ==========================================
st.title("👥 Sistema de Gestión de Colaboradores")

# Splash Screen de entrada
if "_primera_carga" not in st.session_state:
    st.markdown("""
    <div id="splash_screen" style="position:fixed;inset:0;z-index:9999999;background:#0a1223;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:30px;animation: fadeout 0.5s ease-in 2.5s forwards;">
        <div style="width:80px;height:80px;border:6px solid rgba(255,255,255,0.1);border-top-color:#3b82f6;border-radius:50%;animation:sp 1s linear infinite;"></div>
        <div style="color:white;font-size:24px;letter-spacing:2px;font-family:sans-serif;">INICIANDO SISTEMA...</div>
    </div>
    <style>@keyframes sp { to { transform: rotate(360deg); } } @keyframes fadeout { 100% { opacity: 0; visibility: hidden; } }</style>
    """, unsafe_allow_html=True)
    st.session_state["_primera_carga"] = True
else:
    # Este markdown vacío es clave para mantener la estructura del árbol de Streamlit
    # y evitar que st.tabs pierda su pestaña activa al usar st.rerun()
    st.markdown('<div style="display:none"></div>', unsafe_allow_html=True)

opciones_nav = [
    "📊 Ver Consolidado Maestro", 
    "🔄 Cargar Nuevos Archivos",
    "📝 Registro de Asistentes"
]

tab1, tab2, tab3 = st.tabs(opciones_nav)

st.divider()

# ==========================================
# 4. CONTENIDO POR PESTAÑA
# ==========================================

with tab1:
    df_actual = st.session_state["df_maestro"]
    if df_actual.empty:
        st.info("ℹ️ Base de datos vacía en BigQuery.")
    else:
        st.metric(label="Total de Colaboradores Registrados", value=len(df_actual))
        st.dataframe(df_actual, use_container_width=True)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_actual.to_excel(writer, index=False)
        st.download_button("📥 Descargar Consolidado (Excel)", data=output.getvalue(), file_name="consolidado.xlsx", type="primary")

with tab2:
    st.subheader("Fusión de datos con BigQuery")
    archivo_nuevo = st.file_uploader("Sube el archivo Excel o CSV", type=["csv", "xlsx"])
    if archivo_nuevo:
        # Lógica de carga
        if archivo_nuevo.name.endswith('.csv'): df_n = pd.read_csv(archivo_nuevo, dtype=str)
        else: df_n = pd.read_excel(archivo_nuevo, dtype=str)
        
        if st.button("🚀 Guardar Definitivamente en BigQuery", type="primary"):
            df_n = sanitizar_dataframe(df_n)
            # Eliminar duplicados antes de subir
            df_n = df_n.drop_duplicates()
            if guardar_maestro(df_n):
                st.success("¡Datos actualizados en la nube!")
                st.session_state["df_maestro"] = df_n

with tab3:
    # --- SCRIPT DE CARGA PROFESIONAL (MEJORADO) ---
    components.html("""
    <script>
        const pd = window.parent.document;
        const app = pd.querySelector('.stApp');
        
        const removeLoader = () => {
            const loader = pd.getElementById('manual-loader');
            if (loader) {
                loader.style.opacity = '0';
                setTimeout(() => { if(loader) loader.remove(); }, 300);
            }
        };

        const handleManualClick = (e) => {
            const btn = e.target.closest('button');
            if (!btn) return;

            // Filtros de navegación
            if (btn.closest('[data-testid="stTab"]') || btn.closest('[data-testid="stRadio"]')) return;
            
            const texto = btn.innerText.toUpperCase();
            
            // DISPARADOR: Solo para Validar, Guardar y Descargar
            if (texto.includes('VALIDAR') || texto.includes('GUARDAR') || texto.includes('DESCARGAR')) {
                if (pd.getElementById('manual-loader')) return;

                const div = pd.createElement('div');
                div.id = 'manual-loader';
                
                // Fondo oscuro (Overlay)
                div.style.cssText = `
                    position: fixed; inset: 0; background: rgba(0, 0, 0, 0.7); 
                    z-index: 999999; display: flex; flex-direction: column; 
                    align-items: center; justify-content: center; 
                    transition: opacity 0.3s ease; opacity: 1;
                `;
                
                // Cuadro de procesamiento
                div.innerHTML = `
                    <div style="background: #111; padding: 45px 70px; border-radius: 15px; border: 1px solid #333; display: flex; flex-direction: column; align-items: center; box-shadow: 0 20px 50px rgba(0,0,0,0.9);">
                        <div style="width:65px; height:65px; border:6px solid rgba(59, 130, 246, 0.1); border-top:6px solid #3b82f6; border-radius:50%; animation: spin 0.8s cubic-bezier(0.5, 0.1, 0.5, 0.9) infinite;"></div>
                        <p style="margin-top:30px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; letter-spacing: 3px; font-weight: 600; color: white; font-size: 15px; text-align: center;">PROCESANDO REGISTRO...</p>
                    </div>
                    <style>@keyframes spin { to { transform: rotate(360deg); } }</style>
                `;
                pd.body.appendChild(div);
                
                // FAILSAFE: Si por algo la red falla, el loader se quita en 50 segundos máximo
                setTimeout(removeLoader, 50000);
            }
        };

        // Escuchar clics
        pd.removeEventListener('click', handleManualClick);
        pd.addEventListener('click', handleManualClick, true);

        // LÓGICA DE CIERRE INTELIGENTE:
        // No se cierra inmediatamente, espera a que Streamlit esté 'IDLE' por al menos 500ms
        let idleCheck;
        const observer = new MutationObserver(() => {
            const isStale = app.getAttribute('data-stale') === 'true';
            if (!isStale) {
                clearTimeout(idleCheck);
                idleCheck = setTimeout(() => {
                    if (app.getAttribute('data-stale') !== 'true') {
                        removeLoader();
                    }
                }, 500); // Se cierra inmediatamente (500ms)
            }
        });

        if (app) {
            observer.observe(app, { attributes: true, attributeFilter: ['data-stale'] });
        }
    </script>
    """, height=0)

    # Renderizado del módulo de registro
    registro_asistentes.render_registro()
