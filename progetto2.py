import altair as alt
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
from queries import *
import streamlit as st
from datetime import datetime
from streamlit_option_menu import option_menu
 
st.set_page_config(
    page_title="Dashboard Voli - Big Data",
    page_icon="✈️",
    layout="wide",
)

# Funzione per convertire un DataFrame Spark in pandas
def spark_to_pandas(spark_df):
    """
    Converte un DataFrame Spark in un DataFrame Pandas, con un limite al numero di righe.
    Necessario per l'integrazione con Streamlit.
    """
    return spark_df.toPandas()


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

def get_coordinates(tratte_pd,coordinate_df):
    coordinates_list = []
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
            coordinates_list.append(coords)
        else:
            if origin_coords.empty:
                st.warning(f"Coordinate mancanti per l'origine: {row['Origin']}")
            if dest_coords.empty:
                st.warning(f"Coordinate mancanti per la destinazione: {row['Dest']}")       
    return pd.DataFrame(coordinates_list)

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

# Menu orizzontale con Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7= st.tabs(["Home", "Rotte", "Aeroporti", "Compagnie","Tempistiche","AI","Paesi"])

# --- Home Page ---
with tab1:
    st.title("Dashboard Voli - Panoramica")
    
    colonne = st.columns((1.5, 4.5, 2), gap='medium')

    #colonna di sx 
    with colonne[0]:
        st.subheader("Anteprima Dataset", divider="grey")

        st.metric(label="Voli totali", value=conta_righe_totali())
        nome_Aereo_piu_km=aereo_piu_km_percorsi().collect()[0]["Tail_Number"]
        
        st.metric(label="Aereo con più km percorsi",value=nome_Aereo_piu_km)
        giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli())
        giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
        numero_voli=giorno_piu_voli.iloc[0]["count"]
        # Converto il numero del giorno nella stringa
        giorno_nome = converti_giorno(giorno_num)
        st.markdown(
        f"""
        <div style="padding: 10px; background-color: lightgrey; border-radius: 10px; font-size: 18px; text-align: center;">
            <b>Giorno della settimana con più voli:</b> {giorno_nome}<br>
            <b>Numero:</b> {numero_voli}
        </div>
        """, unsafe_allow_html=True
        )
        
        st.markdown('#### Percentuali')
        
        st.write('In orario')
        perc_orario=percentuale_voli_orario()
        in_anticipo=make_donut(perc_orario,"Voli in orario","green")
        st.altair_chart(in_anticipo)

        st.write('In ritardo')
        perc_ritardo=percentuale_voli_ritardo()
        in_ritardo=make_donut(perc_ritardo,"Voli in ritardo","red")
        st.altair_chart(in_ritardo)
    
    #colonna centrale
    with colonne[1]:
        dfTem = pd.read_csv(coordinateAeroporti, delimiter=",")
        st.subheader("Mappa", divider="red")

        # Creazione del menu orizzontale con option_menu
        mappa_opzione = option_menu(
            menu_title=None,  # Nessun titolo
            options=["Aeroporti di partenza", "Aeroporti di destinazione", "Aeroporti coinvolti"],  # Opzioni
            icons=["send", "map", "globe"],  # Icone (opzionale)
            default_index=0,  # Indice iniziale
            orientation="horizontal",  # Menu orizzontale
            styles={
                "container": {"padding": "0px", "background-color": "#f8f9fa"},
                "icon": {"color": "light-grey", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
                "nav-link-selected": {"background-color": "light-grey"},
            },
        )
        
        if mappa_opzione=="Aeroporti di partenza":
            # Dati per la mappa
            dfMap1 = pd.DataFrame()
            dfMap1 = dfTem[['OriginLat', 'OriginLon']]
            dfMap1.rename(columns={'OriginLat': 'LAT', 'OriginLon': 'LON'}, inplace=True)
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito un aereo")
            st.map(data=dfMap1, zoom=2)
        
        elif mappa_opzione=="Aeroporti di destinazione":
            dfMap2 = pd.DataFrame()
            dfMap2 = dfTem[['DestinationLat', 'DestinationLon']]
            dfMap2.rename(columns={'DestinationLat': 'LAT', 'DestinationLon': 'LON'}, inplace=True)
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto in cui è atterrato un aereo")
            st.map(data=dfMap2, zoom=2,color="#0000ff")
        
        elif mappa_opzione=="Aeroporti coinvolti":
            dfMap1 = pd.DataFrame()
            dfMap1 = dfTem[['OriginLat', 'OriginLon']]
            dfMap1.rename(columns={'OriginLat': 'LAT', 'OriginLon': 'LON'}, inplace=True)
            dfMap2 = pd.DataFrame()
            dfMap2 = dfTem[['DestinationLat', 'DestinationLon']]
            dfMap2.rename(columns={'DestinationLat': 'LAT', 'DestinationLon': 'LON'}, inplace=True)
            #unico dati sulle partenze e quelli sulle destinazioni
            dfMap = pd.concat([dfMap1, dfMap2])
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito, atterrato o transitato un aereo")
            st.map(data=dfMap, zoom=1,color="#008000")  
        
        #creo altre due colonne in quella centrale per mostrare i grafici
        colonne_interne_centro=st.columns([1,1])
        
        #colonna a sx sotto la mappa 
        with colonne_interne_centro[0]:

            dati_compagnie=spark_to_pandas(compagnie_piu_voli_fatti())
            # Cambio l'indice per impostare le compagnie come asse Y
            dati_compagnie = dati_compagnie.set_index("Reporting_Airline")

            st.subheader("Grafici", divider="red")
            st.markdown('##### Principali compagnie per voli fatti')
            st.bar_chart(data=dati_compagnie,x=None,y=None,color=None,x_label="Numero voli fatti",y_label="Codice compagnia",horizontal=True,use_container_width=False)

            st.markdown('#### Principali cause ritardo')
            percentuali=spark_to_pandas(percentuali_cause_ritardo())
            #traspongo il dataframe cosi lo posso plottare avendo le cause come asse Y
            percentuali = percentuali.transpose()
            st.bar_chart(data=percentuali,x=None,y=None,color=None,x_label="Percentuale",y_label="Causa",horizontal=True,use_container_width=False)

        #colonna a dx sotto la mappa
        with colonne_interne_centro[1]:
            st.subheader("", divider="red")
            st.markdown('##### Stati più puntuali')
            stati_min_ritardo=spark_to_pandas(statiMinRitardoMedio())
            stati_min_ritardo.set_index('DestStateName', inplace=True)
            # stati_min_ritardo=stati_min_ritardo.transpose()
            st.bar_chart(data=stati_min_ritardo,x=None,y=None,color=None,x_label="Ritardo medio",y_label="Stato",horizontal=True,use_container_width=False)
            
            st.markdown('#### Ritardi medi per stagione')
            ritardi_mensili=spark_to_pandas(ritardo_medio_per_stagione())
            st.bar_chart(ritardi_mensili.set_index("Stagione"), horizontal=True,use_container_width=True)
        
        voli_per_mese=spark_to_pandas(totaleVoliPerMese())
        voli_per_mese["Month"] = voli_per_mese["Month"].map(mesi)
       
        voli_per_mese_wide = voli_per_mese.pivot_table(
            index="Month", columns="Month", values="NumeroVoli", aggfunc="sum"
        ).fillna(0)
        
        mesi_ordinati=["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno","Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]   
        voli_per_mese_wide = voli_per_mese_wide.reindex(columns=mesi_ordinati)
        st.markdown('#### Numero di voli per mese')
        # Visualizza il grafico con colori distinti per mese
        st.area_chart(voli_per_mese_wide, use_container_width=True)      


    with colonne[2]:
        st.subheader("Stati più visitati", divider="grey")
        dataframe_query2=spark_to_pandas(stati_piu_visitati())
        st.dataframe(dataframe_query2,
                    column_order=("DestStateName", "count"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "DestStateName": st.column_config.TextColumn(
                            "Stato",
                        ),
                        "count": st.column_config.ProgressColumn(
                            "Voli atterrati",
                            help="Numero di voli atterrati nello stato in relazione ai totali", 
                            format="%f",
                            min_value=0, 
                            max_value=conta_righe_totali() 
                        )}
                    )
        st.markdown('#### Top Rotte Aeree')
        dataframe_query=spark_to_pandas(rottePiuComuni())
        st.dataframe(dataframe_query,
                    column_order=("Origin", "Dest","NumeroVoli"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Origin": st.column_config.TextColumn(
                            "Partenza",
                        ),
                        "Dest": st.column_config.TextColumn(
                            "Arrivo",
                        ),
                        "NumeroVoli": st.column_config.ProgressColumn(
                            "NumeroVoli",
                            format="%f",
                            min_value=0, 
                            max_value=conta_righe_totali() 
                        )
                        }
                    )
        st.markdown('#### Compagnie con piu km percorsi')
        dataframe_query=spark_to_pandas(compagniaPiuKmPercorsi())
        st.dataframe(dataframe_query,
                    column_order=("Reporting_Airline", "TotaleKm"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Reporting_Airline": st.column_config.TextColumn(
                            "Compagnia", 
                            help="Il carrier code delle compagnie",  
                            width="small"
                        ),
                        "TotaleKm": st.column_config.NumberColumn(
                            "Km percorsi",
                            help="Il totale dei km percorsi dalla compagnia",
                            format="%gmi",   
                        )}
                    )

#--- Aeroporti ---        
with tab3:
    st.subheader("Analisi dei dati relativi ai singoli aeroporti")
    # Widget per selezionare un aeroporto
    aeroporto_selezionato = st.selectbox("Seleziona un aeroporto", spark_to_pandas(codici_aeroporti()))
    colonne_tab3 = st.columns((0.75,2), gap='medium')
    with colonne_tab3[0]:
        st.subheader("Info Aeroporto",divider="grey")
        st.metric(label="Città dell'aeroporto",value=trova_citta_da_aeroporto(aeroporto_selezionato))
        st.metric(label="Voli totali passati per l'aeroporto", value=numero_voli_per_aeroporto(aeroporto_selezionato))

        giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli(aeroporto_selezionato))
        giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
        numero_voli=giorno_piu_voli.iloc[0]["count"]
        # Converto il numero del giorno nella stringa
        giorno_nome = converti_giorno(giorno_num)
        st.markdown(
        f"""
        <div style="padding: 10px; background-color: lightgrey; border-radius: 10px; font-size: 18px; text-align: center;">
            <b>Giorno della settimana con più voli:</b> {giorno_nome}<br>
            <b>Numero:</b> {numero_voli}
        </div>
        """, unsafe_allow_html=True
        )

        st.markdown('#### Percentuali')
        st.write('In orario')
        perc_anticipo=percentuale_voli_orario(aeroporto_selezionato)
        in_anticipo=make_donut(perc_anticipo,"Voli in orario","green")
        st.altair_chart(in_anticipo)
        st.write('In ritardo')
        perc_ritardo=percentuale_voli_ritardo(aeroporto_selezionato)
        in_ritardo=make_donut(perc_ritardo,"Voli in ritardo","red")
        st.altair_chart(in_ritardo)
        

        st.markdown("#### Numero di voli per mese")
        voli_mesi_aeroporto=spark_to_pandas(totaleVoliPerMese(aeroporto_selezionato))
        # Aggiungo i nomi dei mesi
        voli_mesi_aeroporto["Month"] = voli_mesi_aeroporto["Month"].map(mesi)
        st.dataframe(voli_mesi_aeroporto,
                    column_order=("Month", "NumeroVoli"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Month": st.column_config.TextColumn(
                            "Mese",
                        ),
                        "NumeroVoli": st.column_config.ProgressColumn(
                            "Voli transitati",
                            help="Numero di voli transitati nell'aeroporto nei singoli mesi, la progress bar è in relazione ai totali passati dall'aeroporto", 
                            format="%f",
                            min_value=0, 
                            max_value=numero_voli_per_aeroporto(aeroporto_selezionato) 
                        )}
                    )
        
        st.markdown("#### Totale voli cancellati dall'aeroporto")
        st.metric(label="Voli totali cancellati dall'aeroporto", value=totale_voli_cancellati(aeroporto=aeroporto_selezionato))
        
        st.markdown("#### Voli cancellati per mese")
        voli_cancellati_mese_aeroporto=spark_to_pandas(totale_voli_cancellati_per_mese(aeroporto_selezionato))  
        voli_cancellati_mese_aeroporto["Month"] = voli_cancellati_mese_aeroporto["Month"].map(mesi)  
        st.dataframe(voli_cancellati_mese_aeroporto,
                    column_order=("Month", "NumeroVoliCancellati"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Month": st.column_config.TextColumn(
                            "Mese",
                        ),
                        "NumeroVoliCancellati": st.column_config.ProgressColumn(
                            "Voli cancellati",
                            help="Numero di voli cancellati nell'aeroporto nei singoli mesi, la progress bar è in relazione ai totali cancellati dall'aeroporto", 
                            format="%f",
                            min_value=0, 
                            max_value=totale_voli_cancellati(aeroporto=aeroporto_selezionato) 
                        )}
                    )


        
    with colonne_tab3[1]:
        st.subheader("Analisi Compagnia",divider="red") 
        direzione=st.segmented_control("Direzione",["Da","Verso"],selection_mode="single",default="Da")
        mappa_opzione = option_menu(
            menu_title=None,  # Nessun titolo
            options=["Volo più distante", "Volo più vicino", "Tratta più percorsa"],  # Opzioni
            icons=["send", "map", "globe"],  # Icone (opzionale)
            default_index=0,  # Indice iniziale
            orientation="horizontal",  # Menu orizzontale
            styles={
                "container": {"padding": "0px", "background-color": "#f8f9fa"},
                "icon": {"color": "light-grey", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
                "nav-link-selected": {"background-color": "light-grey"},
            },
            key='mappa_opzione_aeroporto'
        ) 
        if direzione=="Da":
            if mappa_opzione=="Volo più distante":
                volo_distanza_max_da=spark_to_pandas(volo_distanza_max_da_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_distanza_max_da.iloc[0]["OriginCityName"]
                partenza_code=volo_distanza_max_da.iloc[0]["Origin"]
                citta_arrivo=volo_distanza_max_da.iloc[0]["DestCityName"]
                dest_code=volo_distanza_max_da.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_distanza_max_da, coordinate_df)
                st.markdown(f"Tratta con distanza max partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_max_da.iloc[0]['Distance']} miglia")
                disegna_tratta(tratte_coords,colonne_tab3[1])
            elif mappa_opzione=="Volo più vicino":
                volo_distanza_min_da=spark_to_pandas(volo_distanza_min_da_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_distanza_min_da.iloc[0]["OriginCityName"]
                partenza_code=volo_distanza_min_da.iloc[0]["Origin"]
                citta_arrivo=volo_distanza_min_da.iloc[0]["DestCityName"]
                dest_code=volo_distanza_min_da.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_distanza_min_da, coordinate_df)
                st.markdown(f"Tratta con distanza min partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_min_da.iloc[0]['Distance']} miglia")
                disegna_tratta(tratte_coords,colonne_tab3[1])
            elif mappa_opzione=="Tratta più percorsa":
                volo_piu_percorso_da=spark_to_pandas(tratta_piu_percorsa_da_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_piu_percorso_da.iloc[0]["OriginCityName"]
                partenza_code=volo_piu_percorso_da.iloc[0]["Origin"]
                citta_arrivo=volo_piu_percorso_da.iloc[0]["DestCityName"]
                dest_code=volo_piu_percorso_da.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_piu_percorso_da, coordinate_df)
                st.markdown(f"Tratta più percorsa: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_da.iloc[0]['count']} volte ")
                disegna_tratta(tratte_coords,colonne_tab3[1])
        else:#verso
            if mappa_opzione=="Volo più distante":
                volo_distanza_max_per=spark_to_pandas(volo_distanza_max_verso_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_distanza_max_per.iloc[0]["OriginCityName"]
                partenza_code=volo_distanza_max_per.iloc[0]["Origin"]
                citta_arrivo=volo_distanza_max_per.iloc[0]["DestCityName"]
                dest_code=volo_distanza_max_per.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_distanza_max_per, coordinate_df)
                st.markdown(f"Tratta con distanza max partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_max_per.iloc[0]['Distance']} miglia")
                disegna_tratta(tratte_coords,colonne_tab3[1])
            elif mappa_opzione=="Volo più vicino":
                volo_distanza_min_per=spark_to_pandas(volo_distanza_min_verso_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_distanza_min_per.iloc[0]["OriginCityName"]
                partenza_code=volo_distanza_min_per.iloc[0]["Origin"]
                citta_arrivo=volo_distanza_min_per.iloc[0]["DestCityName"]
                dest_code=volo_distanza_min_per.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_distanza_min_per, coordinate_df)
                st.markdown(f"Tratta con distanza min partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_min_per.iloc[0]['Distance']} miglia")
                disegna_tratta(tratte_coords,colonne_tab3[1])
            elif mappa_opzione=="Tratta più percorsa":
                volo_piu_percorso_per=spark_to_pandas(tratta_piu_percorsa_verso_aeroporto(aeroporto_selezionato))
                citta_partenza=volo_piu_percorso_per.iloc[0]["OriginCityName"]
                partenza_code=volo_piu_percorso_per.iloc[0]["Origin"]
                citta_arrivo=volo_piu_percorso_per.iloc[0]["DestCityName"]
                dest_code=volo_piu_percorso_per.iloc[0]["Dest"]
                coordinate_df=pd.read_csv(coordinate_aeroporto)
                tratte_coords = get_coordinates(volo_piu_percorso_per, coordinate_df)
                st.markdown(f"Tratta più percorsa: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_per.iloc[0]['count']} volte ")
                disegna_tratta(tratte_coords,colonne_tab3[1])
        ##SEZIONE GRAFICI   
        st.subheader("Grafici", divider="red")
        st.markdown('#### Principali cause ritardo')
        #PERCENTUALE RITARDO
        selezione_data_cause_ritardi=st.slider("Seleziona il periodo",value=[datetime(2013,1,1),datetime(2013,12,31)],format="DD-MM-YYYY",min_value=datetime(2013,1,1),max_value=datetime(2013,12,31))

        percentuali_ritardo_aeroporto=spark_to_pandas(percentuali_cause_ritardo( filtro_compagnia=None,causa_specifica=None,data_inizio=selezione_data_cause_ritardi[0].date(),data_fine=selezione_data_cause_ritardi[1].date(),stato=None,aeroporto=aeroporto_selezionato))
        percentuali_ritardo_aeroporto = percentuali_ritardo_aeroporto.transpose()
        st.bar_chart(data=percentuali_ritardo_aeroporto,x=None,y=None,color=None,x_label="Percentuale",y_label="Causa",horizontal=True,use_container_width=True)
        
        st.markdown('#### Ritardi al decollo aeroporto')
        st.markdown("Il grafico mostra la distribuzione dei ritardi al decollo per l'aeroporto. È possibile scegliere se visualizzare i ritardi mensili o giornalieri ed eventualmente specificare il periodo di interesse. Inoltre a sinistra è possibile selezionare uno o più aeroporti differenti con cui confrontare i ritardi.")
        colonne_confronto_e_periodo=st.columns(2)
        with colonne_confronto_e_periodo[0]:
            mese_giornaliero=st.segmented_control("Periodo",["Mensile","Giornaliero"],selection_mode="single",default="Mensile")
        with colonne_confronto_e_periodo[1]:
            confronto=st.multiselect("Aeroporto per confronto",spark_to_pandas(codici_aeroporti()),key='confronto_ritardi')#metto la key altrimenti streamlit da problemi se non lo distinguo da quello sotto che ha stessi parametri ma serve per vedere numero voli nelle date importanti
        colonne_slider=st.columns(2)
        dati_aeroporti = {}
        if mese_giornaliero=="Mensile":
            with colonne_slider[0]:
                mese_inizio = st.slider("Seleziona il mese di inizio", 1, 12, value=1)
            with colonne_slider[1]:
                mese_fine = st.slider("Seleziona il mese di fine", 1, 12, value=12)
            # ritardi_al_decollo_aeroporto=spark_to_pandas(calcolo_ritardo_per_mesi(aeroporto_selezionato,mese_inizio,mese_fine))
            # #QUESTE DUE ISTRUZIONI SONO EQUIVALENTI E MI PERMETTONO DI MAPPARE I RITARDI MEDI SUI MESI CHE USERO' COME ASCISSE DEL GRAFICO
            # #IN PARTICOLARE CONSIDERANDO L'INTERO MESE MAPPO CON UNA DATA CHE PARTE DAL PRIMO DI QUEL MESE NELL'ANNO 2013
            # ritardi_al_decollo_aeroporto['Data'] = pd.to_datetime(ritardi_al_decollo_aeroporto['Month'], format='%m').map(lambda d: d.replace(year=2013)) 
            # #ritardi_al_decollo_aeroporto['date'] = pd.to_datetime(ritardi_al_decollo_aeroporto[['Month']].assign(YEAR=2013,DAY=1))
            # st.line_chart(data=ritardi_al_decollo_aeroporto,x='Data',y='ritardo_medio')
            dati_principale = spark_to_pandas(calcolo_ritardo_per_mesi(aeroporto_selezionato,mese_inizio,mese_fine))
            dati_principale['Data'] = pd.to_datetime(dati_principale['Month'], format='%m').map(lambda d: d.replace(year=2013))
            dati_principale = dati_principale[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": aeroporto_selezionato})
            dati_aeroporti[aeroporto_selezionato] = dati_principale
            for aeroporto in confronto:
                if aeroporto != aeroporto_selezionato:
                    dati_confronto = spark_to_pandas(calcolo_ritardo_per_mesi(aeroporto, mese_inizio, mese_fine))
                    dati_confronto['Data'] = pd.to_datetime(dati_confronto['Month'], format='%m').map(lambda d: d.replace(year=2013))
                    #anche qui rinomino le colonne come prima per poter fare il confronto e avere una legenda dei grafici chiara e ognuno col suo colore
                    dati_confronto = dati_confronto[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": aeroporto})
                    dati_aeroporti[aeroporto] = dati_confronto
            # Unisco tutti i dati in un unico DataFrame, cosi se è stata scelta l'opzione per il confronto li plotto tutti
            chart_data = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if chart_data.empty:
                    chart_data = dati.set_index("Data")
                else:
                    chart_data = chart_data.join(dati.set_index("Data"), how="outer")
            #in automatico streamlit dal dataframe capisce cosa fare e mi crea il grafico con colori separati e dati passati 
            st.line_chart(chart_data)

        else: #giornaliero
            with colonne_slider[0]:
                data_inizio = st.date_input("Seleziona la data di inizio", value=pd.to_datetime("2013-01-01"),format="DD-MM-YYYY")
            with colonne_slider[1]:
                data_fine = st.date_input("Seleziona la data di fine", value=pd.to_datetime("2013-12-31"),format="DD-MM-YYYY")
            #Ottengo i dati per l'aeroporto selezionato e gli aeroporti per confronto
            dati_principale = spark_to_pandas(calcola_ritardo_giornaliero(aeroporto_selezionato, str(data_inizio), str(data_fine)))
            dati_principale['Data'] = pd.to_datetime(dati_principale['FlightDate'])
            #Rinonimino la colonna titardo col nome dell'aeroporto cosi se faccio confronti so a quale mi riferisco
            dati_principale = dati_principale[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": aeroporto_selezionato})
            dati_aeroporti[aeroporto_selezionato] = dati_principale
            for aeroporto in confronto:
                if aeroporto != aeroporto_selezionato:
                    dati_confronto = spark_to_pandas(calcola_ritardo_giornaliero(aeroporto, str(data_inizio), str(data_fine)))
                    dati_confronto['Data'] = pd.to_datetime(dati_confronto['FlightDate'])
                    #anche qui rinomino le colonne come prima per poter fare il confronto e avere una legenda dei grafici chiara e ognuno col suo colore
                    dati_confronto = dati_confronto[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": aeroporto})
                    dati_aeroporti[aeroporto] = dati_confronto
            # Unisco tutti i dati in un unico DataFrame, cosi se è stata scelta l'opzione per il confronto li plotto tutti
            chart_data = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if chart_data.empty:
                    chart_data = dati.set_index("Data")
                else:
                    chart_data = chart_data.join(dati.set_index("Data"), how="outer")
            #in automatico streamlit dal dataframe capisce cosa fare e mi crea il grafico con colori separati e dati passati 
            st.line_chart(chart_data)

    
        st.markdown('#### Operatività nei giorni più importanti dell\'anno')
        grafico_a_barre_opzione = option_menu(
            menu_title=None,  # Nessun titolo
            options=["Voli partiti", "Voli atterrati", "Voli totali","Percentuale Cancellazioni"],  # Opzioni
            icons=["send", "map", "globe", "x-circle"],  # Icone (opzionale)
            default_index=0,  # Indice iniziale
            orientation="horizontal",  # Menu orizzontale
            styles={
                "container": {"padding": "0px", "background-color": "#f8f9fa"},
                "icon": {"color": "light-grey", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
                "nav-link-selected": {"background-color": "light-grey"},
            },
        )
        #VALUTIAMO VOLI NEGLI AEROPORTI NEI GIORNI DI:
        #Indipendenza, Natale, Vigilia di Natale, Ferragosto, Halloween, Giorno del Ringraziamento, Giorno prima del giorno del ringraziamento, Giorno di San Valentino, Black Friday
        date_importanti=["2013-07-04","2013-12-25","2013-12-24","2013-08-15","2013-10-31","2013-11-23","2013-11-22","2013-02-14","2013-11-29"]
        confronto_date_importanti=st.multiselect("Aeroporto per confronto",spark_to_pandas(codici_aeroporti()),key='date_importanti')
        


        if grafico_a_barre_opzione == "Voli partiti":
            # Create a dictionary to store all airports' data
            dati_aeroporti = {}
                
            # Process main airport data
            voli_date_importanti = {}
            # Calculate daily average for main airport
            totale_voli_anno = totale_voli_da_aeroporto(aeroporto_selezionato)
            media_giornaliera = totale_voli_anno / 365
            
            # Add the daily average as a separate "date" entry
            voli_date_importanti["Media Giornaliera"] = media_giornaliera
            
            # Add data for each important date
            for data in date_importanti:
                num_voli = totale_voli_da_aeroporto(aeroporto_selezionato, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                voli_date_importanti[data_formattata] = num_voli
            
            # Create DataFrame for main airport
            df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                      columns=['Data', aeroporto_selezionato])
            df_principale.set_index('Data', inplace=True)
            dati_aeroporti[aeroporto_selezionato] = df_principale

            # Process comparison airports
            for aeroporto in confronto_date_importanti:
                if aeroporto != aeroporto_selezionato:
                    voli_confronto = {}
                    # Calculate daily average for comparison airport
                    totale_voli_anno_confronto = totale_voli_da_aeroporto(aeroporto)
                    media_giornaliera_confronto = totale_voli_anno_confronto / 365
                    
                    # Add the daily average as a separate "date" entry
                    voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                    
                    # Add data for each important date
                    for data in date_importanti:
                        num_voli = totale_voli_da_aeroporto(aeroporto, data)
                        data_dt = pd.to_datetime(data)
                        data_formattata = data_dt.strftime('%d %B')
                        voli_confronto[data_formattata] = num_voli
                    
                    df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                             columns=['Data', aeroporto])
                    df_confronto.set_index('Data', inplace=True)
                    dati_aeroporti[aeroporto] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            # Add explanatory text
            st.markdown("""
            Il grafico mostra il numero di voli partiti dall'aeroporto nelle date principali dell'anno.
            L'ultima barra rappresenta la media giornaliera dei voli per ogni aeroporto, permettendo un confronto 
            immediato tra l'operatività normale e quella nei giorni speciali.
            """)
            
            # Create the bar chart
            st.bar_chart(data=df_combinato, use_container_width=True)
        elif grafico_a_barre_opzione == "Voli atterrati":
             # Create a dictionary to store all airports' data
            dati_aeroporti = {}
                
            # Process main airport data
            voli_date_importanti = {}

            # Calculate daily average for main airport
            
            totale_voli_anno = totale_voli_verso_aeroporto(aeroporto_selezionato)
            media_giornaliera = totale_voli_anno / 365
            
            # Add the daily average as a separate "date" entry
            voli_date_importanti["Media Giornaliera"] = media_giornaliera
            
            # Add data for each important date
            for data in date_importanti:
                num_voli = totale_voli_verso_aeroporto(aeroporto_selezionato, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                voli_date_importanti[data_formattata] = num_voli
            
            # Create DataFrame for main airport
            df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                      columns=['Data', aeroporto_selezionato])
            df_principale.set_index('Data', inplace=True)
            dati_aeroporti[aeroporto_selezionato] = df_principale

            # Process comparison airports
            for aeroporto in confronto_date_importanti:
                if aeroporto != aeroporto_selezionato:
                    voli_confronto = {}
                    # Calculate daily average for comparison airport
                    totale_voli_anno_confronto = totale_voli_verso_aeroporto(aeroporto)
                    media_giornaliera_confronto = totale_voli_anno_confronto / 365
                    
                    # Add the daily average as a separate "date" entry
                    voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                    
                    # Add data for each important date
                    for data in date_importanti:
                        num_voli = totale_voli_verso_aeroporto(aeroporto, data)
                        data_dt = pd.to_datetime(data)
                        data_formattata = data_dt.strftime('%d %B')
                        voli_confronto[data_formattata] = num_voli
                    
                    df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                             columns=['Data', aeroporto])
                    df_confronto.set_index('Data', inplace=True)
                    dati_aeroporti[aeroporto] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            # Add explanatory text
            st.markdown("""
            Il grafico mostra il numero di voli arrivati nell'aeroporto nelle date principali dell'anno.
            L'ultima barra rappresenta la media giornaliera dei voli per ogni aeroporto, permettendo un confronto 
            immediato tra l'operatività normale e quella nei giorni speciali.
            """)
            
            # Create the bar chart
            st.bar_chart(data=df_combinato, use_container_width=True)
        elif grafico_a_barre_opzione == "Voli totali":
            # Create a dictionary to store all airports' data
            dati_aeroporti = {}
                
            # Process main airport data
            voli_date_importanti = {}

            # Calculate daily average for main airport
            
            totale_voli_anno = numero_voli_per_aeroporto(aeroporto_selezionato)
            media_giornaliera = totale_voli_anno / 365
            
            # Add the daily average as a separate "date" entry
            voli_date_importanti["Media Giornaliera"] = media_giornaliera
            
            # Add data for each important date
            for data in date_importanti:
                num_voli = numero_voli_per_aeroporto(aeroporto_selezionato, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                voli_date_importanti[data_formattata] = num_voli
            
            # Create DataFrame for main airport
            df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                      columns=['Data', aeroporto_selezionato])
            df_principale.set_index('Data', inplace=True)
            dati_aeroporti[aeroporto_selezionato] = df_principale

            # Process comparison airports
            for aeroporto in confronto_date_importanti:
                if aeroporto != aeroporto_selezionato:
                    voli_confronto = {}
                    # Calculate daily average for comparison airport
                    totale_voli_anno_confronto = numero_voli_per_aeroporto(aeroporto)
                    media_giornaliera_confronto = totale_voli_anno_confronto / 365
                    
                    # Add the daily average as a separate "date" entry
                    voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                    
                    # Add data for each important date
                    for data in date_importanti:
                        num_voli = numero_voli_per_aeroporto(aeroporto, data)
                        data_dt = pd.to_datetime(data)
                        data_formattata = data_dt.strftime('%d %B')
                        voli_confronto[data_formattata] = num_voli
                    
                    df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                             columns=['Data', aeroporto])
                    df_confronto.set_index('Data', inplace=True)
                    dati_aeroporti[aeroporto] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            # Add explanatory text
            st.markdown("""
            Il grafico mostra il numero di voli totali passati per l'aeroporto nelle date principali dell'anno.
            L'ultima barra rappresenta la media giornaliera dei voli totali per ogni aeroporto, permettendo un confronto 
            immediato tra l'operatività normale e quella nei giorni speciali.
            """)
            
            # Create the bar chart
            st.bar_chart(data=df_combinato, use_container_width=True)
        elif grafico_a_barre_opzione == "Percentuale Cancellazioni":
            dati_aeroporti = {}
            cancellazioni_date_importanti = {}
            # Calculate yearly cancellation percentage for main airline
            cancellazioni_anno = percentuale_voli_cancellati_aeroporto(aeroporto_selezionato)
            cancellazioni_date_importanti["Media Annuale"] = cancellazioni_anno
            
            # Add data for each important date
            for data in date_importanti:
                cancellazioni = percentuale_voli_cancellati_aeroporto(aeroporto_selezionato, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                cancellazioni_date_importanti[data_formattata] = cancellazioni
            
            df_principale = pd.DataFrame(
                list(cancellazioni_date_importanti.items()), 
                columns=['Data', aeroporto_selezionato]
            )
            df_principale.set_index('Data', inplace=True)
            dati_aeroporti[aeroporto_selezionato] = df_principale

            # Process comparison airlines
            for aeroporto in confronto_date_importanti:
                cancellazioni_confronto = {}
                
                df_cancellazioni_anno = percentuale_voli_cancellati_aeroporto(aeroporto)
                
                cancellazioni_confronto["Media Annuale"] = df_cancellazioni_anno
                
                for data in date_importanti:
                    cancellazioni = percentuale_voli_cancellati_aeroporto(aeroporto, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    cancellazioni_confronto[data_formattata] = cancellazioni
                
                df_confronto = pd.DataFrame(
                    list(cancellazioni_confronto.items()),
                    columns=['Data', aeroporto]
                )
                df_confronto.set_index('Data', inplace=True)
                dati_aeroporti[aeroporto] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for aeroporto, dati in dati_aeroporti.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            st.markdown("""
            Il grafico mostra la percentuale di voli cancellati nell'aeroporto nelle date principali dell'anno.
            L'ultima barra rappresenta la media annuale delle cancellazioni, permettendo un confronto 
            tra le cancellazioni normali e quelle nei giorni speciali.
            """)
            
            st.bar_chart(data=df_combinato, use_container_width=True)


        
        st.subheader("Aeroporti collegati", divider="red")
        st.markdown("Il grafico mostra le rotte aeree che collegano l'aeroporto selezionato ad altri aeroporti")
        aeroporti_collegati= spark_to_pandas(tratte_distinte_per_aeroporto(aeroporto_selezionato))
        coordinate_df = pd.read_csv(coordinate_aeroporto)
        tratte_coords = get_coordinates(aeroporti_collegati, coordinate_df)
        disegna_tratta(tratte_coords, colonne_tab3[1])
        
#--- Compagnie ---
with tab4:
    st.subheader("Analisi dei dati relativi alle singole compagnie aeree")
    # Widget per selezionare una compagnia
    compagnia_selezionata = st.selectbox("Seleziona una compagnia", spark_to_pandas(codici_compagnie_aeree()))
    colonne_tab4 = st.columns((0.75,2), gap='medium')
    with colonne_tab4[0]:
        st.subheader("Info Compagnie",divider="grey")
        
        #MAGARI METTERE CITTA PIU VISITATA DALLA COMPAGNIA
        #st.metric(label="Città dell'aeroporto",value=trova_citta_da_aeroporto(compagnia_selezionata))
        st.metric(label="Voli totali effettuati dalla compagnia", value=numero_voli_per_compagnia(compagnia_selezionata))

        giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli(compagnia=compagnia_selezionata))
        giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
        numero_voli=giorno_piu_voli.iloc[0]["count"]
        # Converto il numero del giorno nella stringa
        giorno_nome = converti_giorno(giorno_num)
        st.markdown(
        f"""
        <div style="padding: 10px; background-color: lightgrey; border-radius: 10px; font-size: 18px; text-align: center;">
            <b>Giorno della settimana con più voli:</b> {giorno_nome}<br>
            <b>Numero:</b> {numero_voli}
        </div>
        """, unsafe_allow_html=True
        )

        st.markdown('#### Percentuali')
        st.write('In orario')
        perc_anticipo=percentuale_voli_orario(compagnia=compagnia_selezionata)
        in_anticipo=make_donut(perc_anticipo,"Voli in orario","green")
        st.altair_chart(in_anticipo)
        st.write('In ritardo')
        perc_ritardo=percentuale_voli_ritardo(compagnia=compagnia_selezionata)
        in_ritardo=make_donut(perc_ritardo,"Voli in ritardo","red")
        st.altair_chart(in_ritardo)

        st.markdown("#### Numero di voli per mese effetutati dalla compagnia")
        voli_mesi_compagnia=spark_to_pandas(totaleVoliPerMese(compagnia=compagnia_selezionata))
        # Aggiungo i nomi dei mesi
        voli_mesi_compagnia["Month"] = voli_mesi_compagnia["Month"].map(mesi)
        st.dataframe(voli_mesi_compagnia,
                    column_order=("Month", "NumeroVoli"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Month": st.column_config.TextColumn(
                            "Mese",
                        ),
                        "NumeroVoli": st.column_config.ProgressColumn(
                            "Voli effettuati",
                            help="Numero di voli effettuati dalla compagnia nei singoli mesi, la progress bar è in relazione ai totali effettuati dalla compagnia", 
                            format="%f",
                            min_value=0, 
                            max_value=numero_voli_per_compagnia(compagnia_selezionata) 
                        )}
                    )
        
        st.markdown("#### Totale voli cancellati dalla compagnia")
        st.metric(label="Voli totali cancellati dalla compagnia", value=totale_voli_cancellati(compagnia=compagnia_selezionata))
        
        st.markdown("#### Voli cancellati per mese")
        voli_cancellati_mese_compagnia=spark_to_pandas(totale_voli_cancellati_per_mese(compagnia=compagnia_selezionata))  
        voli_cancellati_mese_compagnia["Month"] = voli_cancellati_mese_compagnia["Month"].map(mesi)  
        st.dataframe(voli_cancellati_mese_compagnia,
                    column_order=("Month", "NumeroVoliCancellati"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "Month": st.column_config.TextColumn(
                            "Mese",
                        ),
                        "NumeroVoliCancellati": st.column_config.ProgressColumn(
                            "Voli cancellati",
                            help="Numero di voli cancellati dalla compagnia nei singoli mesi, la progress bar è in relazione ai totali cancellati dalla compagnia", 
                            format="%f",
                            min_value=0, 
                            max_value=totale_voli_cancellati(compagnia=compagnia_selezionata) 
                        )}
                    )
    with colonne_tab4[1]:
        st.subheader("Analisi Aeroporto",divider="red") 

        mappa_opzione_compagnie = option_menu(
            menu_title=None,  # Nessun titolo
            options=["Volo più distante", "Volo più vicino", "Tratta più percorsa"],  # Opzioni
            icons=["send", "map", "globe"],  # Icone (opzionale)
            default_index=0,  # Indice iniziale
            orientation="horizontal",  # Menu orizzontale
            styles={
                "container": {"padding": "0px", "background-color": "#f8f9fa"},
                "icon": {"color": "light-grey", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
                "nav-link-selected": {"background-color": "light-grey"},
            },
            key="mappa_opzione_compagnie"
        ) 
        if mappa_opzione_compagnie=="Volo più distante":
            volo_max_compagnia=spark_to_pandas(volo_distanza_max_compagnia(compagnia_selezionata))
            citta_partenza=volo_max_compagnia.iloc[0]["OriginCityName"]
            partenza_code=volo_max_compagnia.iloc[0]["Origin"]
            citta_arrivo=volo_max_compagnia.iloc[0]["DestCityName"]
            dest_code=volo_max_compagnia.iloc[0]["Dest"]
            coordinate_df=pd.read_csv(coordinate_aeroporto)
            tratte_coords = get_coordinates(volo_max_compagnia, coordinate_df)
            st.markdown(f"Tratta con distanza max effettuata dalla compagnia partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_max_compagnia.iloc[0]['Distance']} miglia")
            disegna_tratta(tratte_coords,colonne_tab4[1])
        elif mappa_opzione_compagnie=="Volo più vicino":
            volo_min_dist_compagnia=spark_to_pandas(volo_distanza_min_compagnia(compagnia_selezionata))
            citta_partenza=volo_min_dist_compagnia.iloc[0]["OriginCityName"]
            partenza_code=volo_min_dist_compagnia.iloc[0]["Origin"]
            citta_arrivo=volo_min_dist_compagnia.iloc[0]["DestCityName"]
            dest_code=volo_min_dist_compagnia.iloc[0]["Dest"]
            coordinate_df=pd.read_csv(coordinate_aeroporto)
            tratte_coords = get_coordinates(volo_min_dist_compagnia, coordinate_df)
            st.markdown(f"Tratta con distanza min effettuata dalla compagnia partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_min_dist_compagnia.iloc[0]['Distance']} miglia")
            disegna_tratta(tratte_coords,colonne_tab4[1])
        elif mappa_opzione_compagnie=="Tratta più percorsa":
            volo_piu_percorso_comp=spark_to_pandas(tratta_piu_percorsa_compagnia(compagnia_selezionata))
            citta_partenza=volo_piu_percorso_comp.iloc[0]["OriginCityName"]
            partenza_code=volo_piu_percorso_comp.iloc[0]["Origin"]
            citta_arrivo=volo_piu_percorso_comp.iloc[0]["DestCityName"]
            dest_code=volo_piu_percorso_comp.iloc[0]["Dest"]
            coordinate_df=pd.read_csv(coordinate_aeroporto)
            tratte_coords = get_coordinates(volo_piu_percorso_comp, coordinate_df)
            st.markdown(f"Tratta più percorsa dalla compagnia: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_comp.iloc[0]['count']} volte ")
            disegna_tratta(tratte_coords,colonne_tab4[1])
#######################################################

        ##SEZIONE GRAFICI   
        st.subheader("Grafici", divider="red")
        st.markdown('#### Principali cause ritardo')
        #PERCENTUALE RITARDO
        selezione_data_cause_ritardi_compagnia = st.slider(
            "Seleziona il periodo",
            value=[datetime(2013,1,1), datetime(2013,12,31)],
            format="DD-MM-YYYY",
            min_value=datetime(2013,1,1),
            max_value=datetime(2013,12,31),
            key="date_slider_airline"
        )

        percentuali_ritardo_compagnia = spark_to_pandas(percentuali_cause_ritardo(filtro_compagnia=compagnia_selezionata,causa_specifica=None,data_inizio=selezione_data_cause_ritardi_compagnia[0].date(),data_fine=selezione_data_cause_ritardi_compagnia[1].date(),stato=None,aeroporto=None))
        percentuali_ritardo_compagnia = percentuali_ritardo_compagnia.transpose()
        st.bar_chart(data=percentuali_ritardo_compagnia,x=None,y=None,color=None,x_label="Percentuale",y_label="Causa",horizontal=True,use_container_width=True)
        
        st.markdown('#### Ritardi medi per compagnia')
        st.markdown("Il grafico mostra l'andamento dei ritardi medi della compagnia. È possibile confrontare con altre compagnie e visualizzare i dati mensili o giornalieri.")
        
        colonne_confronto_e_periodo_comp = st.columns(2)
        with colonne_confronto_e_periodo_comp[0]:
            mese_giornaliero_comp = st.segmented_control(
                "Periodo",
                ["Mensile", "Giornaliero"],
                selection_mode="single",
                default="Mensile",
                key="period_control_airline"
            )
        with colonne_confronto_e_periodo_comp[1]:
            confronto_comp = st.multiselect(
                "Compagnie per confronto",
                spark_to_pandas(codici_compagnie_aeree()),
                key='confronto_ritardi_compagnie'
            )

        colonne_slider_comp = st.columns(2)
        dati_compagnie = {}

        if mese_giornaliero_comp == "Mensile":
            with colonne_slider_comp[0]:
                mese_inizio_comp = st.slider("Seleziona il mese di inizio", 1, 12, value=1, key="month_start_airline")
            with colonne_slider_comp[1]:
                mese_fine_comp = st.slider("Seleziona il mese di fine", 1, 12, value=12, key="month_end_airline")

            # Process data for selected airline
            dati_principale = spark_to_pandas(calcolo_ritardo_per_mesi_compagnia(compagnia=compagnia_selezionata, mese_inizio=mese_inizio_comp, mese_fine=mese_fine_comp))
            dati_principale['Data'] = pd.to_datetime(dati_principale['Month'], format='%m').map(lambda d: d.replace(year=2013))
            dati_principale = dati_principale[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": compagnia_selezionata})
            dati_compagnie[compagnia_selezionata] = dati_principale

            # Process data for comparison airlines
            for compagnia in confronto_comp:
                if compagnia != compagnia_selezionata:
                    dati_confronto = spark_to_pandas(calcolo_ritardo_per_mesi_compagnia(compagnia=compagnia, mese_inizio=mese_inizio_comp, mese_fine=mese_fine_comp))
                    dati_confronto['Data'] = pd.to_datetime(dati_confronto['Month'], format='%m').map(lambda d: d.replace(year=2013))
                    dati_confronto = dati_confronto[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": compagnia})
                    dati_compagnie[compagnia] = dati_confronto

        else:  # Giornaliero
            with colonne_slider_comp[0]:
                data_inizio_comp = st.date_input("Seleziona la data di inizio", value=pd.to_datetime("2013-01-01"), format="DD-MM-YYYY", key="date_start_airline")
            with colonne_slider_comp[1]:
                data_fine_comp = st.date_input("Seleziona la data di fine", value=pd.to_datetime("2013-12-31"), format="DD-MM-YYYY", key="date_end_airline")

            # Process data for selected airline
            dati_principale = spark_to_pandas(calcola_ritardo_giornaliero_compagnia(compagnia=compagnia_selezionata, data_inizio=str(data_inizio_comp), data_fine=str(data_fine_comp)))
            dati_principale['Data'] = pd.to_datetime(dati_principale['FlightDate'])
            dati_principale = dati_principale[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": compagnia_selezionata})
            dati_compagnie[compagnia_selezionata] = dati_principale

            # Process data for comparison airlines
            for compagnia in confronto_comp:
                if compagnia != compagnia_selezionata:
                    dati_confronto = spark_to_pandas(calcola_ritardo_giornaliero_compagnia(compagnia=compagnia, data_inizio=str(data_inizio_comp), data_fine=str(data_fine_comp)))
                    dati_confronto['Data'] = pd.to_datetime(dati_confronto['FlightDate'])
                    dati_confronto = dati_confronto[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": compagnia})
                    dati_compagnie[compagnia] = dati_confronto

        # Combine all data for visualization
        chart_data = pd.DataFrame()
        for compagnia, dati in dati_compagnie.items():
            if chart_data.empty:
                chart_data = dati.set_index("Data")
            else:
                chart_data = chart_data.join(dati.set_index("Data"), how="outer")

        st.line_chart(chart_data)

        st.markdown('#### Operatività nei giorni importanti dell\'anno')
        grafico_giorni_importanti = option_menu(
            menu_title=None,
            options=["Numero voli", "Percentuale cancellazioni"],
            icons=["airplane", "x-circle"],
            default_index=0,
            orientation="horizontal",
            styles={
                "container": {"padding": "0px", "background-color": "#f8f9fa"},
                "icon": {"color": "light-grey", "font-size": "18px"},
                "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
                "nav-link-selected": {"background-color": "light-grey"},
            },
            key="important_days_airline"
        )

        date_importanti = [
            "2013-07-04", "2013-12-25", "2013-12-24", "2013-08-15", 
            "2013-10-31", "2013-11-23", "2013-11-22", "2013-02-14", "2013-11-29"
        ]
        confronto_date_importanti_comp = st.multiselect(
            "Compagnie per confronto",
            spark_to_pandas(codici_compagnie_aeree()),
            key='date_importanti_compagnie'
        )

        if grafico_giorni_importanti == "Numero voli":
            dati_compagnie = {}
            voli_date_importanti = {}
            
            # Calculate daily average for main airline
            totale_voli_anno = numero_voli_per_compagnia(compagnia_selezionata)
            media_giornaliera = totale_voli_anno / 365
            voli_date_importanti["Media Giornaliera"] = media_giornaliera
            
            # Add data for each important date
            for data in date_importanti:
                num_voli = numero_voli_per_compagnia(compagnia_selezionata, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                voli_date_importanti[data_formattata] = num_voli
            
            df_principale = pd.DataFrame(
                list(voli_date_importanti.items()), 
                columns=['Data', compagnia_selezionata]
            )
            df_principale.set_index('Data', inplace=True)
            dati_compagnie[compagnia_selezionata] = df_principale

            # Process comparison airlines
            for compagnia in confronto_date_importanti_comp:
                voli_confronto = {}
                totale_voli_anno = numero_voli_per_compagnia(compagnia)
                media_giornaliera = totale_voli_anno / 365
                voli_confronto["Media Giornaliera"] = media_giornaliera
                
                for data in date_importanti:
                    num_voli = numero_voli_per_compagnia(compagnia, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(
                    list(voli_confronto.items()),
                    columns=['Data', compagnia]
                )
                df_confronto.set_index('Data', inplace=True)
                dati_compagnie[compagnia] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for compagnia, dati in dati_compagnie.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            st.markdown("""
            Il grafico mostra il numero di voli operati dalla compagnia nelle date principali dell'anno.
            L'ultima barra rappresenta la media giornaliera dei voli, permettendo un confronto 
            tra l'operatività normale e quella nei giorni speciali.
            """)
            
            st.bar_chart(data=df_combinato, use_container_width=True)

        else:  # Percentuale cancellazioni
            dati_compagnie = {}
            cancellazioni_date_importanti = {}
            # Calculate yearly cancellation percentage for main airline
            cancellazioni_anno = percentuale_voli_cancellati_compagnia(compagnia_selezionata)
            cancellazioni_date_importanti["Media Annuale"] = cancellazioni_anno
            
            # Add data for each important date
            for data in date_importanti:
                cancellazioni = percentuale_voli_cancellati_compagnia(compagnia_selezionata, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                cancellazioni_date_importanti[data_formattata] = cancellazioni
            
            df_principale = pd.DataFrame(
                list(cancellazioni_date_importanti.items()), 
                columns=['Data', compagnia_selezionata]
            )
            df_principale.set_index('Data', inplace=True)
            dati_compagnie[compagnia_selezionata] = df_principale

            # Process comparison airlines
            for compagnia in confronto_date_importanti_comp:
                cancellazioni_confronto = {}
                
                df_cancellazioni_anno = percentuale_voli_cancellati_compagnia(compagnia)
                
                cancellazioni_confronto["Media Annuale"] = df_cancellazioni_anno
                
                for data in date_importanti:
                    cancellazioni = percentuale_voli_cancellati_compagnia(compagnia, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    cancellazioni_confronto[data_formattata] = cancellazioni
                
                df_confronto = pd.DataFrame(
                    list(cancellazioni_confronto.items()),
                    columns=['Data', compagnia]
                )
                df_confronto.set_index('Data', inplace=True)
                dati_compagnie[compagnia] = df_confronto

            # Combine all DataFrames
            df_combinato = pd.DataFrame()
            for compagnia, dati in dati_compagnie.items():
                if df_combinato.empty:
                    df_combinato = dati
                else:
                    df_combinato = df_combinato.join(dati)

            st.markdown("""
            Il grafico mostra la percentuale di voli cancellati dalla compagnia nelle date principali dell'anno.
            L'ultima barra rappresenta la media annuale delle cancellazioni, permettendo un confronto 
            tra le cancellazioni normali e quelle nei giorni speciali.
            """)
            
            st.bar_chart(data=df_combinato, use_container_width=True)
                
        st.subheader("Tratte eseguite dalla compagnia", divider="red")
        st.markdown("Il grafico mostra le rotte aeree eseguite dalla compagnia")
        tratte_percorse_compagnia= spark_to_pandas(tratte_distinte_per_compagnia(compagnia_selezionata))
        coordinate_df = pd.read_csv(coordinate_aeroporto)
        tratte_coords = get_coordinates(tratte_percorse_compagnia, coordinate_df)
        disegna_tratta(tratte_coords, colonne_tab4[1])


    
    

# Footer
st.markdown("---")
st.write("Progetto Big Data - Analisi Ritardi Voli ✈️")