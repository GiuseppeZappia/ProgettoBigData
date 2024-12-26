
# Layout della pagina

import altair as alt
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
from queries import *
import streamlit as st
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


# Menu orizzontale con Tabs
tab1, tab2, tab3, tab4, tab5= st.tabs(["Home", "Analisi Ritardi", "Mappa Voli", "Grafici Personalizzati","Machine Learning"])

# --- Home Page ---
with tab1:
    st.title("Dashboard Voli - Panoramica")
    
    colonne = st.columns((1.5, 4.5, 2), gap='medium')

    with colonne[0]:
        st.subheader("Anteprima Dataset", divider="grey")
        st.metric(label="Voli totali", value=conta_righe_totali())
        nome_Aereo_piu_km=aereo_piu_km_percorsi().collect()[0]["Tail_Number"]
        st.metric(label="Aereo con più km percorsi",value=nome_Aereo_piu_km)
        giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli())
        # giorno_num = giorno_piu_voli.collect()[0]["DayOfWeek"]
        # numero_voli = giorno_piu_voli.collect()[0]["count"]
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
        perc_anticipo=percentuale_voli_anticipo()
        in_anticipo=make_donut(perc_anticipo,"Voli in orario","green")
        st.altair_chart(in_anticipo)
        st.write('In ritardo')
        perc_ritardo=percentuale_voli_ritardo()
        in_ritardo=make_donut(perc_ritardo,"Voli in ritardo","red")
        st.altair_chart(in_ritardo)
        # st.plotly_chart(widget_percentuale_rotondo(perc_anticipo,"Voli in anticipo"))
        # Tabella con un'anteprima del dataset
    
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
            # Data for the map
            dfMap1 = pd.DataFrame()
            dfMap1 = dfTem[['OriginLat', 'OriginLon']]
            dfMap1.rename(columns={'OriginLat': 'LAT', 'OriginLon': 'LON'}, inplace=True)
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito un aereo")
            st.map(data=dfMap1, zoom=2)
        elif mappa_opzione=="Aeroporti di destinazione":
            # Data for the map
            dfMap2 = pd.DataFrame()
            dfMap2 = dfTem[['DestinationLat', 'DestinationLon']]
            dfMap2.rename(columns={'DestinationLat': 'LAT', 'DestinationLon': 'LON'}, inplace=True)
            # First row - map
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto in cui è atterrato un aereo")
            st.map(data=dfMap2, zoom=2,color="#0000ff")
        elif mappa_opzione=="Aeroporti coinvolti":
            # Data for the map
            dfMap1 = pd.DataFrame()
            dfMap1 = dfTem[['OriginLat', 'OriginLon']]
            dfMap1.rename(columns={'OriginLat': 'LAT', 'OriginLon': 'LON'}, inplace=True)
            dfMap2 = pd.DataFrame()
            dfMap2 = dfTem[['DestinationLat', 'DestinationLon']]
            dfMap2.rename(columns={'DestinationLat': 'LAT', 'DestinationLon': 'LON'}, inplace=True)
            dfMap = pd.concat([dfMap1, dfMap2])
            st.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito, atterrato o transitato un aereo")
            st.map(data=dfMap, zoom=1,color="#008000")  
        
        colonne_interne_centro=st.columns([1,1])
        
        with colonne_interne_centro[0]:
            dati_compagnie=spark_to_pandas(compagnie_piu_voli_fatti())
            # Cambia l'indice per impostare le compagnie come asse Y
            dati_compagnie = dati_compagnie.set_index("Reporting_Airline")
            st.subheader("Grafici", divider="red")
            st.markdown('##### Principali compagnie per voli fatti')
            st.bar_chart(data=dati_compagnie,x=None,y=None,color=None,x_label="Numero voli fatti",y_label="Codice compagnia",horizontal=True,use_container_width=False)

            st.markdown('#### Principali cause ritardo')
            #PERCENTUALE RITARDO
            percentuali=spark_to_pandas(percentuali_cause_ritardo())
            # Riorganizzare i dati con .melt() per avere le percentuali sulle ascisse
            percentuali = percentuali.transpose()
            st.bar_chart(data=percentuali,x=None,y=None,color=None,x_label="Percentuale",y_label="Causa",horizontal=True,use_container_width=False)

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
        voli_per_mese_wide = voli_per_mese.pivot_table(
            index="Month", columns="Month", values="NumeroVoli", aggfunc="sum"
        ).fillna(0)

        # Mappatura dei numeri dei mesi ai nomi
        mesi = {
            1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
            5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
            9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
        }

        # Aggiungi i nomi dei mesi
        voli_per_mese["Month"] = voli_per_mese["Month"].map(mesi)
        # Imposta i mesi come indice per il grafico
        voli_per_mese_wide.columns = voli_per_mese["Month"].unique()  # Rinomina colonne per chiarezza

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




# Footer
st.markdown("---")
st.write("Progetto Big Data - Analisi Ritardi Voli ✈️")