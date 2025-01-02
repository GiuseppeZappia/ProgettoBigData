import pandas as pd
from queries import *
import streamlit as st
from streamlit_option_menu import option_menu
from utils import *


st.set_page_config(
    page_title="Dashboard Voli - Big Data",
    page_icon="✈️",
    layout="wide",
)

st.title("Dashboard Voli - Panoramica")

# CSS per i widget
st.markdown(
    """
    <style>
    .stat-widget {
        background-color: #f4f4f4;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.1);
    }
    .stat-title {
        font-size: 16px;
        font-weight: bold;
        margin-bottom: 8px;
    }
    .stat-value {
        font-size: 24px;
        color: #2c3e50;
        font-weight: bold;
    }
    </style>
    """, 
    unsafe_allow_html=True
)



colonne = st.columns((2, 5.2, 2), gap='large')
with colonne[0]:

    st.subheader("Anteprima Dataset",divider="grey")
    st.metric(label="Voli totali", value=conta_righe_totali())
    nome_Aereo_piu_km=aereo_piu_km_percorsi().collect()[0]["Tail_Number"]
    
    st.metric(label="Aereo con più miglia percorse",value=nome_Aereo_piu_km)
    giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli())
    giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
    numero_voli=giorno_piu_voli.iloc[0]["count"]
    # Converto il numero del giorno in stringa
    giorno_nome = converti_giorno(giorno_num)
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Giorno della settimana con più voli:</div>
            <div class="stat-value">{giorno_nome} - {numero_voli}</div>
        </div>
        """,
        unsafe_allow_html=True
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

with colonne[1]:
    dfTem = pd.read_csv(coordinateAeroporti, delimiter=",")
    st.subheader("Mappa", divider="red")
    mappa_opzione = option_menu(
        menu_title=None,
        options=["Aeroporti di partenza", "Aeroporti di destinazione", "Aeroporti coinvolti"],  # Opzioni
        icons=["send", "map", "globe"],
        default_index=0, 
        orientation="horizontal", 
        styles={
            "container": {"padding": "0px", "background-color": "#f8f9fa"},
            "icon": {"color": "light-grey", "font-size": "18px"},
            "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
            "nav-link-selected": {"background-color": "light-grey"},
        },
    )
    
    if mappa_opzione=="Aeroporti di partenza":
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
        dfMap = pd.concat([dfMap1, dfMap2])
        st.markdown("Ogni punto sulla mappa rappresenta un aeroporto da cui è partito, atterrato o transitato un aereo")
        st.map(data=dfMap, zoom=1,color="#008000")  
    
    colonne_interne_centro=st.columns([1,1])
    
    with colonne_interne_centro[0]:
        dati_compagnie=spark_to_pandas(compagnie_piu_voli_fatti())
        dati_compagnie = dati_compagnie.set_index("Reporting_Airline")
        
        st.subheader("Grafici", divider="red")
        st.markdown('##### Principali compagnie per voli fatti')
        st.bar_chart(data=dati_compagnie,x=None,y=None,color=None,x_label="Numero voli fatti",y_label="Codice compagnia",horizontal=True,use_container_width=False)
        st.markdown('#### Principali cause ritardo')
        
        percentuali=spark_to_pandas(percentuali_cause_ritardo())
        percentuali = percentuali.transpose()
        st.bar_chart(data=percentuali,x=None,y=None,color=None,x_label="Percentuale",y_label="Causa",horizontal=True,use_container_width=False)

    with colonne_interne_centro[1]:
        st.subheader("", divider="red")
        st.markdown('##### Stati più puntuali')
        
        stati_min_ritardo=spark_to_pandas(statiMinRitardoMedio())
        stati_min_ritardo.set_index('DestStateName', inplace=True)
        
        st.bar_chart(data=stati_min_ritardo,x=None,y=None,color=None,x_label="Ritardo medio",y_label="Stato",horizontal=True,use_container_width=False)
        st.markdown('#### Ritardi medi per stagione')
        
        ritardi_mensili=spark_to_pandas(ritardo_medio_per_stagione())
        st.bar_chart(ritardi_mensili.set_index("Stagione"), horizontal=True,use_container_width=True)
    
    voli_per_mese=spark_to_pandas(totaleVoliPerMese())
    voli_per_mese["Month"] = voli_per_mese["Month"].map(mesi)
   
    voli_per_mese_wide = voli_per_mese.pivot_table(
        index="Month", columns="Month", values="NumeroVoli", aggfunc="sum"
    ).fillna(0)
     
    voli_per_mese_wide = voli_per_mese_wide.reindex(columns=mesi_ordinati)
    st.markdown('#### Numero di voli per mese')

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
    st.markdown('#### Compagnie con più miglia percorse')
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
                        "Miglia percorse",
                        help="Il totale delle miglia percorse dalla compagnia",
                        format="%gmi",   
                    )}
                )

