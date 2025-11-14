from utils.job_runner import JobRunner
from utils.loader_local import LoaderLocal
from utils.transformation import Transformation
from utils.writer_local import WriterLocal


class RefGareIdfJob(JobRunner):
    def __init__(self):
        self.in_path = "/home/onyxia/work/hackathon_mobilites_2025/data/raw/emplacement-des-gares-idf.csv"
        self.out_path = "/home/onyxia/work/hackathon_mobilites_2025/data/interim/ref_gares.gpq"

    def process(self):
        # Lecture des données à partir d'un fichier csv
        df = LoaderLocal.loader_csv(self.in_path)

        # Transformation des données et sélection des colonnes utiles
        df["station_clean"] = df["nom_zda"].apply(Transformation.clean_name)
        cols = ["geo_point_2d", "id_ref_zdc", "id_ref_zda", "nom_zda",
                "station_clean", "res_com", "mode", "exploitant"]
        df_selected = df[cols]

        # Conversion du dataframe Pandas en GeoPandas
        # Séparation des coordonnées
        coords = df_selected["geo_point_2d"].str.split(",", expand=True)
        df_selected = df_selected.copy()
        df_selected["latitude"] = coords[0].astype(float)
        df_selected["longitude"] = coords[1].astype(float)

        gdf_final = Transformation.transform_geopandas(df_selected, "latitude", "longitude")

        # Ecriture des données en format geoparquet
        WriterLocal.write_geoparquet(gdf_final, self.out_path)
