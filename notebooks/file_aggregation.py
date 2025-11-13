import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Dictionnaire des fichiers
dico = {
    'Etablissements adultes handicapés': 'hackathon_mobilites_2025/data/etablissements_et_services_pour_adultes_handicapes.csv',
    'Etablissements enfants handicapés': 'hackathon_mobilites_2025/data/etablissements_et_services_pour_l_enfance_et_la_jeunesse_handicapee.csv',
    'Etablissements hospitaliers': 'hackathon_mobilites_2025/data/les_etablissements_hospitaliers_franciliens.csv'
}


def col_extract(dico):
    dataframes = []

    for type_etablissement, filepath in dico.items():
        df = pd.read_csv(filepath, sep=';')

        # Sélectionner les colonnes utile
        col_map = {
            'lat': ['lat', 'LAT'],
            'lng': ['lng', 'LNG'],
            'RAISON_SOCIALE': ['RAISON_SOCIALE']
        }

        def find_col(df_cols, candidates):
            for cand in candidates:
                if cand in df.columns:
                    return cand
            return None

        lat_col = find_col(df.columns, col_map['lat'])
        lng_col = find_col(df.columns, col_map['lng'])
        rs_col = find_col(df.columns, col_map['RAISON_SOCIALE'])

        if not all([lat_col, lng_col, rs_col]):
            print(f"Colonnes manquantes dans {type_etablissement}: lat={lat_col}, lng={lng_col}, RAISON_SOCIALE={rs_col}")
            continue

        # Extraire et renommer
        subset = df[[lat_col, lng_col, rs_col]].copy()
        subset.rename(columns={
            lat_col: 'lat',
            lng_col: 'lng',
            rs_col: 'RAISON_SOCIALE'
        }, inplace=True)

        # Supprimer les lignes avec coordonnées manquantes
        subset.dropna(subset=['lat', 'lng'], inplace=True)

        # Ajouter la colonne de type
        subset['type_etablissement'] = type_etablissement

        dataframes.append(subset)

    if not dataframes:
        raise ValueError("Aucun DataFrame valide n'a été créé.")

    # Concaténation
    global_df = pd.concat(dataframes, ignore_index=True)

    # Création de la colonne géométrique
    global_df['geometry'] = global_df.apply(
        lambda row: Point(row['lng'], row['lat']), axis=1
    )

    # Conversion en GeoDataFrame (CRS WGS84 = EPSG:4326)
    gdf = gpd.GeoDataFrame(global_df, geometry='geometry', crs='EPSG:4326')

    # Supprimer les colonnes 'lat'/'lng'
    gdf = gdf.drop(columns=['lat', 'lng'])

    return gdf


# Exécution
if __name__ == "__main__":
    gdf = col_extract(dico)
    print(f"GeoDataFrame créé avec {len(gdf)} entités.")
    print(gdf.head())

    # Export au format GeoParquet (nécessite pyarrow + geopandas >= 0.10)
    output_path = 'hackathon_mobilites_2025/data/etablissements_geoparquet.gpq'
    gdf.to_parquet(output_path)
    print(f"Exporté vers : {output_path}")
