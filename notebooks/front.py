# app.py
import streamlit as st
import geopandas as gpd
from pathlib import Path
import folium
from streamlit_folium import st_folium

# ==============================
# 1. Configuration & titre
# ==============================
st.set_page_config(
    page_title="établissements médico-sociaux - Île-de-France",
    page_icon="🏥",
    layout="wide"
)

st.title("📍 Carte des établissements médico-sociaux")
st.markdown("Carte interactive des établissements pour adultes/enfants handicapés et hôpitaux franciliens.")

# ==============================
# 2. Chargement des données
# ==============================
@st.cache_data
def load_data():
    # Chemin relatif robuste (car on est probablement dans notebooks/ ou à la racine)
    # On cherche à partir du répertoire du script
    script_dir = Path(__file__).parent
    candidates = [
        script_dir / "data" / "etablissements_geoparquet.gpq",
        script_dir.parent / "data" / "etablissements_geoparquet.gpq",
        Path("..") / "data" / "etablissements_geoparquet.gpq",
    ]
    
    gdf = None
    for path in candidates:
        if path.exists():
            with st.spinner("Génération de l'itinéraire:"):
                gdf = gpd.read_parquet(path)
                st.success(f"✅ Données chargées !")

    # S'assurer qu'on est en WGS84 (lat/lon)
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


gdf = load_data()

st.markdown("---")
# ==============================
# 4. Carte interactive avec Folium
# ==============================
st.subheader("Carte interactive")

# Centrage sur l'Île-de-France (~Paris)
m = folium.Map(
    location=[48.8566, 2.3522],
    zoom_start=9,
    tiles="CartoDB positron",  # ou "OpenStreetMap"
    attr="Données : Ministère des Solidarités | Carte : © OpenStreetMap"
)

# Ajouter les points
for idx, row in gdf.iterrows():
    # Récupérer les infos
    raison = row.get("RAISON_SOCIALE", "–") or "–"
    etab_type = row.get("type_etablissement", "–") or "–"
    
    # Popup (au clic)
    popup_html = f"""
    <b>{raison}</b><br>
    <i>{etab_type}</i>
    """
    
    # Tooltip (au survol)
    tooltip_html = f"{raison} — {etab_type}"
    
    # Coordonnées
    lon, lat = row.geometry.x, row.geometry.y
    
    # Icône selon le type (optionnel)
    icon_color = {
        "Etablissements adultes handicapés": "blue",
        "Etablissements enfants handicapés": "green",
        "Etablissements hospitaliers": "red"
    }.get(etab_type, "gray")
    
    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=tooltip_html,
        icon=folium.Icon(color=icon_color, icon="info-sign")
    ).add_to(m)

# Afficher la carte dans Streamlit
st_folium(m, width="100%", height=600)
