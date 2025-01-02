import altair as alt
import pandas as pd
import pydeck as pdk
from queries import *
import streamlit as st


# Funzione per convertire un DataFrame Spark in pandas
def spark_to_pandas(spark_df):
    return spark_df.toPandas()

@st.cache_data(show_spinner=False)
def load_coordinate_data():
    return pd.read_csv(coordinate_aeroporto)

@st.cache_data(show_spinner=False)
def load_airport_codes():
    return spark_to_pandas(codici_aeroporti())

def make_donut(input_response, input_text, input_color):
  if input_color == 'blue':
      chart_color = ['#29b5e8', '#155F7A']
  if input_color == 'green':
      chart_color = ['#27AE60', '#12783D']
  if input_color == 'orange':
      chart_color = ['#F39C12', '#875A12']
  if input_color == 'red':
      chart_color = ['#E74C3C', '#781F16']
    
  source = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100-input_response, input_response]
  })
  source_bg = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100, 0]
  })
    
  plot = alt.Chart(source).mark_arc(innerRadius=45, cornerRadius=25).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          #domain=['A', 'B'],
                          domain=[input_text, ''],
                          # range=['#29b5e8', '#155F7A']),  # 31333F
                          range=chart_color),
                      legend=None),
  ).properties(width=130, height=130)
    
  text = plot.mark_text(align='center', color="#29b5e8", font="Lato", fontSize=20, fontWeight=700).encode(text=alt.value(f'{input_response} %'))
  plot_bg = alt.Chart(source_bg).mark_arc(innerRadius=45, cornerRadius=20).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          # domain=['A', 'B'],
                          domain=[input_text, ''],
                          range=chart_color),  # 31333F
                      legend=None),
  ).properties(width=130, height=130)
  return plot_bg + plot + text

@st.cache_data(show_spinner=False)
def get_coordinates(tratte_pd,coordinate_df):
    lista_coordinate = []
    for _, row in tratte_pd.iterrows():
        origin_coords = coordinate_df[coordinate_df["Origin"] == row["Origin"]]
        dest_coords = coordinate_df[coordinate_df["Origin"] == row["Dest"]]
        if not origin_coords.empty and not dest_coords.empty:
            coords = {"OriginLatitude": origin_coords.iloc[0]["OriginLat"],
                            "OriginLongitude": origin_coords.iloc[0]["OriginLon"],
                            "DestLatitude": dest_coords.iloc[0]["OriginLat"],
                            "DestLongitude": dest_coords.iloc[0]["OriginLon"],
                            "outbound": 1  # Valore per la larghezza dell'arco
                            }
            lista_coordinate.append(coords)
        else:
            if origin_coords.empty:
                st.warning(f"Coordinate mancanti per l'origine: {row['Origin']}")
            if dest_coords.empty:
                st.warning(f"Coordinate mancanti per la destinazione: {row['Dest']}")       
    return pd.DataFrame(lista_coordinate)

def disegna_tratta(coord_df,colonna_dove_disegnare):
    colonna_dove_disegnare.pydeck_chart(
                    pdk.Deck(
                        map_style="mapbox://styles/mapbox/light-v10",
                        initial_view_state=pdk.ViewState(
                            latitude=coord_df.iloc[0]["OriginLatitude"],  
                            longitude=coord_df.iloc[0]["OriginLongitude"],
                            zoom=5,
                            pitch=50,
                            height=250,
                        ),
                        layers=[
                            pdk.Layer(
                                "ArcLayer",
                                data=coord_df,
                                get_source_position=["OriginLongitude", "OriginLatitude"],
                                get_target_position=["DestLongitude", "DestLatitude"],
                                get_source_color=[255, 0, 0, 160],
                                get_target_color=[0, 0, 255, 160],
                                auto_highlight=True,
                                width_scale=0.0001,
                                get_width="outbound",
                                width_min_pixels=3,
                                width_max_pixels=30,

                                radius=200,
                                elevation_scale=4,
                                elevation_range=[0, 1000],
                                pickable=True,
                                extruded=True,
                            ),
                        ],
                    )
                )

def converti_giorno(numero):
    giorni = {
        1: "Lunedì",
        2: "Martedì",
        3: "Mercoledì",
        4: "Giovedì",
        5: "Venerdì",
        6: "Sabato",
        7: "Domenica"
    }
    return giorni.get(numero, "Sconosciuto")

# Mappatura dei numeri dei mesi ai nomi
mesi = {1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"}

mesi_ordinati = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio","Giugno", "Luglio", "Agosto", "Settembre", "Ottobre","Novembre", "Dicembre"]

