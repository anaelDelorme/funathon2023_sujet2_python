import streamlit as st
import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy import text
from geoalchemy2 import Geometry, WKTElement
import os
import s3fs
import boto3
import folium
from streamlit_folium import folium_static
import branca
import branca.colormap as cm
import tempfile
import pyreadr
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from streamlit_echarts import st_pyecharts
from geopy.geocoders import Nominatim
from pyecharts.charts import TreeMap
from pyecharts.commons.utils import JsCode



fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'https://'+ os.environ['AWS_S3_ENDPOINT']},
                    key = os.environ["AWS_ACCESS_KEY_ID"], 
                    secret = os.environ["AWS_SECRET_ACCESS_KEY"], 
                    token = os.environ["AWS_SESSION_TOKEN"])   

BUCKET = "projet-funathon"

# Une personnalisation sympa pour l'onglet
st.set_page_config(page_title="Les parcelles agri", page_icon="🌱", layout="wide")
st.title('Les parcelles agricoles 🌱')

# Add a selectbox to the sidebar:
address = st.sidebar.text_input("Entrez une adresse")
if address == "":
    address = 'complexe agricole auzeville tolosane'


# Add a slider to the sidebar:
rayon = st.sidebar.slider(
    "Taille du rond autour de l'adresse (rayon en mètre)",
    10, 50000, 10000
)

def get_coordinates(address):
    geolocator = Nominatim(user_agent="anael.delorme")  # Initialise le géocodeur avec l'identifiant d'application
    location = geolocator.geocode(address)  # Géocode l'adresse
    try:
        location = geolocator.geocode(address)
        if location.latitude is not None:
            latitude = location.latitude
            longitude = location.longitude
            return latitude, longitude
        else:
            return None
    except Exception as e:
        st.error(f"Une erreur s'est produite : {str(e)}")
        return None
coordinates = get_coordinates(address)

if coordinates:
    st.sidebar.success(f"Coordonnées GPS : {coordinates[0]}, {coordinates[1]}")
    ### Les données sur le point
    lat = [coordinates[0]]
    lon = [coordinates[1]]
    #rayon = [10000]
    
    with st.spinner('Chargement en cours...'):
        @st.cache_data
        def recup_cultures():
            # références des cultures
            FILE_KEY_S3 = "2023/sujet2/diffusion/ign/rpg/REF_CULTURES_GROUPES_CULTURES_2020.csv"
            FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
            with fs.open(FILE_PATH_S3, mode="rb") as file_in:
                lib_group_cult = pd.read_csv(file_in, sep=";")
            lib_group_cult_agrege = lib_group_cult.copy()
            lib_group_cult_agrege = lib_group_cult_agrege[['CODE_GROUPE_CULTURE', 'LIBELLE_GROUPE_CULTURE']].drop_duplicates()
            lib_group_cult_agrege = lib_group_cult_agrege.rename(columns = {'CODE_GROUPE_CULTURE':'code_group', 'LIBELLE_GROUPE_CULTURE':'Culture'})
            lib_group_cult_agrege['code_group'] = lib_group_cult_agrege['code_group'].astype('str')
            return(lib_group_cult_agrege)
        lib_group_cult_agrege = recup_cultures()

        @st.cache_data
        def liste_dep():
            FILE_KEY_S3 = "2023/sujet2/diffusion/ign/adminexpress_cog_simpl_000_2023.gpkg"
            FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
            with fs.open(FILE_PATH_S3, mode="rb") as file_in:
                dep = gpd.read_file(file_in, layer = "departement")
            dep = dep.to_crs('EPSG:2154')
            dep2 = dep.copy()
            dep2.geometry = dep2.geometry.buffer(200)
            return(dep2)
        liste_departements = liste_dep()
        

        df = pd.DataFrame({'lon': lon, 'lat': lat, 'rayon': rayon})
        df = gpd.GeoDataFrame(df, geometry= gpd.points_from_xy(df.lon, df.lat, crs='epsg:4326'))
        df = df.to_crs('EPSG:2154')
        df['coord_pt_gps'] = df['geometry'].apply(lambda geom: geom.wkt)
        
        db_connection_url = "postgresql://" + os.environ['PG_URL_RPG']
        engine = create_engine(db_connection_url)
        df.to_postgis('point_anael', con = engine,  if_exists='replace')

        query_prox = "SELECT row_number() OVER () AS row_id, p.coord_pt_gps, p.rayon, r.* FROM public.point_anael p, rpg.parcelles r WHERE ST_DWithin(p.geometry, r.geom, p.rayon);"
        points = gpd.read_postgis(query_prox, engine, geom_col='geom')

        culture_prox = pd.merge(points, lib_group_cult_agrege, how='left', on='code_group')
        culture_prox = culture_prox[['surf_parc', 'nom_com', 'geom', 'Culture']]
        culture_prox = culture_prox.rename(columns = {'surf_parc':'Surface de la parcelle (ha)', 'nom_com' : 'Nom de la commune'})

        # Mapping des valeurs de caractère à des valeurs numériques
        unique_values = list(set(culture_prox["Culture"]))  # Liste des valeurs uniques de la colonne "Culture"
        value_to_number = {value: index for index, value in enumerate(unique_values)}
        numeric_values = [value_to_number[value] for value in culture_prox["Culture"]]

        
        type_de_culture = {'Blé tendre' : 'Céréales',
                'Maïs grain et ensilage' : 'Céréales',
                'Orge' : 'Céréales',
                'Autres céréales' : 'Céréales',
                'Colza' : 'Oléagineux',
                'Tournesol' : 'Oléagineux',
                'Autres oléagineux' : 'Oléagineux',
                'Protéagineux' : 'Autres cultures',
                'Plantes à fibres': 'Autres cultures',
                'Gel (surfaces gelées sans production)' : 'Pas de culture',
                'Riz': 'Autres cultures',
                'Légumineuses à grains': 'Autres cultures',
                'Fourrage' : 'Prairies, fourrage, estives et landes',
                'Estives et landes' : 'Prairies, fourrage, estives et landes',
                'Prairies permanentes' : 'Prairies, fourrage, estives et landes',
                'Prairies temporaires' : 'Prairies, fourrage, estives et landes',
                'Vergers': 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles',
                'Vignes': 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles',
                'Fruits à coque': 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles',
                'Oliviers': 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles',
                'Autres cultures industrielles': 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles',
                'Légumes ou fleurs': 'Légumes ou fleurs',
                'Canne à sucre': 'Autres cultures',
                'Divers': 'Autres cultures'}


        def style_function(feature):
            culture = feature["properties"]["Culture"]
            if type_de_culture[culture] == 'Prairies, fourrage, estives et landes':
                color = "Green"
            elif type_de_culture[culture] == "Céréales":
                color = "Yellow"
            elif type_de_culture[culture] == 'Oléagineux':
                color = "Brown"
            elif type_de_culture[culture] == 'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles':
                color = "Purple"
            elif type_de_culture[culture] == 'Pas de culture':
                color = "Blue"
            else:
                color = "Grey"
            return {
                    "fillOpacity": 0.7,
                    "weight": 0,
                    "fillColor": color,
                    #"fillColor": "black",
                    "color": "#D9D9D9"
            }    

        colonnes_tooltip = ['Surface de la parcelle (ha)', 'Nom de la commune', 'Culture']
        
        try:
            m = folium.Map(location=[lat[0], lon[0]], zoom_start=12, tiles="cartodb positron",)
            folium.Marker(
                [lat[0], lon[0]], tooltip="Point sélectionné",
                icon=folium.Icon(color="green")
            ).add_to(m)
            folium.GeoJson(data=culture_prox, style_function=style_function, tooltip=folium.features.GeoJsonTooltip(
                        fields=colonnes_tooltip,
                        aliases=colonnes_tooltip,
                        sticky=True,
                        opacity=0.9,
                        direction='right',
                    )).add_to(m) 
            
        except Exception as e:
            st.error(f"Une erreur s'est produite, vérifiez que votre adresse est bien en France métropolitaine.")


        
        st.sidebar.markdown(
            """<br/>La carte présente les parcelles autour de l'adresse saisie avec les couleurs :   <br/>   
        <span style='background-color:Green; display:inline-block; width:20px; height:20px;'></span> Prairies, fourrage, estives et landes      
        <span style='background-color:Yellow; display:inline-block; width:20px; height:20px;'></span> Céréales      
        <span style='background-color:Brown; display:inline-block; width:20px; height:20px;'></span> Oléagineux  
        <span style='background-color:Purple; display:inline-block; width:20px; height:20px;'></span> Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles     
            <span style='background-color:Blue; display:inline-block; width:20px; height:20px;'></span> Pas de culture (gel)     
            <span style='background-color:Grey; display:inline-block; width:20px; height:20px;'></span> Autres cultures : protéaginaux, légumes, fleurs...""",
            unsafe_allow_html=True
            )



        folium_static(m, width=1000, height=600)
        
        #st.dataframe(culture_prox)
        departement_du_point = df.sjoin(liste_departements, how="left", predicate='intersects')

        @st.cache_data
        def creer_stat_dep():
            FILE_KEY_S3 = "2023/sujet2/diffusion/resultats/stat_group_cult_by_dep.rds"
            FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
            s3 = boto3.client("s3",
                    endpoint_url = 'https://'+ os.environ['AWS_S3_ENDPOINT'],
                    aws_access_key_id= os.environ["AWS_ACCESS_KEY_ID"], 
                    aws_secret_access_key= os.environ["AWS_SECRET_ACCESS_KEY"], 
                    aws_session_token = os.environ["AWS_SESSION_TOKEN"])
            response = s3.get_object(Bucket=BUCKET, Key=FILE_KEY_S3)
            rds_data = response['Body'].read()
            with tempfile.NamedTemporaryFile() as tmp:
                tmp.write(rds_data)
                result = pyreadr.read_r(tmp.name)
                stat_dep_pt = result[None]
            return(stat_dep_pt)
        stat_departement = creer_stat_dep()

        @st.cache_data
        def creer_stat_france():
            FILE_KEY_S3 = "2023/sujet2/diffusion/resultats/stat_group_cult_fm.csv"
            FILE_PATH_S3 = BUCKET + "/" + FILE_KEY_S3
            with fs.open(FILE_PATH_S3, mode="rb") as file_in:
                stat_fm = pd.read_csv(file_in, sep=",")
            return(stat_fm)
        stat_france = creer_stat_france()

        st.write("### Les cultures du territoire (en surface)")
        type_culture_colors = {
            'Prairies, fourrage, estives et landes': 'green',
            'Céréales': 'yellow',
            'Oléagineux': 'brown',
            'Vergers, vignes, fruits à coque, oliviers et autres cultures industrielles': 'purple',
            'Pas de culture': 'blue',
            'Légumes ou fleurs': 'grey',
            'Autres cultures': 'grey'
        }
        surface_pt = (culture_prox.groupby('Culture', as_index=False)
                .agg(Surface_totale=('Surface de la parcelle (ha)', np.sum))
                .sort_values('Surface_totale', ascending=True))

        surface_pt['groupe_de_culture'] = surface_pt['Culture'].map(type_de_culture) 
        
        
        bar_surface = Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
        bar_surface.add_xaxis(surface_pt.Culture.tolist())
        bar_surface.add_yaxis(
            'Surfaces locales en ha',
            surface_pt.Surface_totale.astype(int).tolist()
        )
        bar_surface.reversal_axis()
        #bar_surface.set_global_opts(visualmap_opts=visual_map)

        st_pyecharts(bar_surface, height=600)
        st.write("### Les 5 principales cultures du territoire (en surface), par rapport au département et à la France")



        stat_pt = (culture_prox.groupby('Culture', as_index=False).agg(Surface_totale=('Surface de la parcelle (ha)', np.sum))
                    .assign(nn=lambda x: x['Surface_totale'].sum()) 
                    .assign(pct_surf_local=lambda x: round(100 * x['Surface_totale'] / x['nn'], 1)) )
        stat_pt = stat_pt[['Culture', 'pct_surf_local']].rename(columns={'pct_surf_local':'Surf. locales (%)'})

        stat_dep_pt_choix = stat_departement[stat_departement.insee_dep == departement_du_point.insee_dep[0]]
        stat_dep_pt_choix = stat_dep_pt_choix[['libelle_groupe_culture', 'pct_surf']]
        stat_dep_pt_choix = stat_dep_pt_choix.rename(columns = {'libelle_groupe_culture':'Culture', 'pct_surf':'Surf. départementales (%)'})

        stat_fm_maj = stat_france.copy()
        stat_fm_maj = stat_fm_maj[['libelle_groupe_culture', 'pct_surf']]
        stat_fm_maj = stat_fm_maj.rename(columns = {'libelle_groupe_culture':'Culture', 'pct_surf':'Surf. nationales(%)'})

        stat_compar = pd.merge(stat_pt, stat_dep_pt_choix, how='outer', on='Culture')
        stat_compar = pd.merge(stat_compar, stat_fm_maj, how='outer', on='Culture')

        stat_compar_graph = stat_compar.sort_values('Surf. locales (%)', ascending=False).iloc[0:5]
        # Créer une instance du graphique en barres
        bar_chart = Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))

        # Données d'exemple
        x_data = stat_compar_graph.Culture.tolist()
        y1_data = stat_compar_graph['Surf. locales (%)'].tolist()
        y2_data = stat_compar_graph['Surf. départementales (%)'].tolist()
        y3_data = stat_compar_graph['Surf. nationales(%)'].tolist()

        # Ajouter les données au graphique
        bar_chart.add_xaxis(x_data)
        bar_chart.add_yaxis('Surfaces locales', y1_data)
        bar_chart.add_yaxis('Surfaces départementales', y2_data)
        bar_chart.add_yaxis('Surfaces nationales', y3_data)

        # Configurer les options des séries
        bar_chart.set_series_opts(
            label_opts=opts.LabelOpts(
                formatter="{c0}%",  # Formater les valeurs en pourcentages
            )
        )
        # Configurer les options de l'axe des x
        bar_chart.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name="Culture",
                axislabel_opts=opts.LabelOpts(rotate=20),
            ),
            yaxis_opts=opts.AxisOpts(
                name="Surfaces occupées",
                axislabel_opts=opts.LabelOpts(formatter=""),
            ),
            legend_opts=opts.LegendOpts(pos_right='center'),
        )

        st_pyecharts(bar_chart, height=600)
        

else:
     st.error("Adresse non trouvée. Veuillez essayer avec une autre adresse.")


