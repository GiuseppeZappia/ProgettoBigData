import pandas as pd
from queries import *
import streamlit as st
from datetime import datetime
from streamlit_option_menu import option_menu
from utils import *


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


st.title("Analisi dei dati relativi alle singole compagnie aeree")

compagnia_selezionata = st.selectbox("Seleziona una compagnia", spark_to_pandas(codici_compagnie_aeree()))
colonne_tab4 = st.columns((0.75,2), gap='medium')

with colonne_tab4[0]:
    st.subheader("Info Compagnie",divider="grey")    
    st.metric(label="Voli totali effettuati dalla compagnia", value=numero_voli_per_compagnia(compagnia_selezionata))

    giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli(compagnia=compagnia_selezionata))
    giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
    numero_voli=giorno_piu_voli.iloc[0]["count"]
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
    st.subheader("Analisi Compagnia",divider="red") 
    
    mappa_opzione_compagnie = option_menu(
        menu_title=None, 
        options=["Volo più distante", "Volo più vicino", "Tratta più percorsa"],
        icons=["send", "map", "globe"], 
        default_index=0, 
        orientation="horizontal", 
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
        coordinate_df=load_coordinate_data()
        tratte_coords = get_coordinates(volo_max_compagnia, coordinate_df)

        st.markdown(f"Tratta con distanza max effettuata dalla compagnia partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_max_compagnia.iloc[0]['Distance']} miglia")

        disegna_tratta(tratte_coords,colonne_tab4[1])

    elif mappa_opzione_compagnie=="Volo più vicino":
        volo_min_dist_compagnia=spark_to_pandas(volo_distanza_min_compagnia(compagnia_selezionata))
        citta_partenza=volo_min_dist_compagnia.iloc[0]["OriginCityName"]
        partenza_code=volo_min_dist_compagnia.iloc[0]["Origin"]
        citta_arrivo=volo_min_dist_compagnia.iloc[0]["DestCityName"]
        dest_code=volo_min_dist_compagnia.iloc[0]["Dest"]
        coordinate_df=load_coordinate_data()
        tratte_coords = get_coordinates(volo_min_dist_compagnia, coordinate_df)
        
        st.markdown(f"Tratta con distanza min effettuata dalla compagnia partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_min_dist_compagnia.iloc[0]['Distance']} miglia")
        
        disegna_tratta(tratte_coords,colonne_tab4[1])
    elif mappa_opzione_compagnie=="Tratta più percorsa":
        volo_piu_percorso_comp=spark_to_pandas(tratta_piu_percorsa_compagnia(compagnia_selezionata))
        citta_partenza=volo_piu_percorso_comp.iloc[0]["OriginCityName"]
        partenza_code=volo_piu_percorso_comp.iloc[0]["Origin"]
        citta_arrivo=volo_piu_percorso_comp.iloc[0]["DestCityName"]
        dest_code=volo_piu_percorso_comp.iloc[0]["Dest"]

        coordinate_df=load_coordinate_data()
        tratte_coords = get_coordinates(volo_piu_percorso_comp, coordinate_df)
        st.markdown(f"Tratta più percorsa dalla compagnia: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_comp.iloc[0]['count']} volte ")
        
        disegna_tratta(tratte_coords,colonne_tab4[1])
 
    st.subheader("Grafici", divider="red")
    st.markdown('#### Principali cause ritardo')

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

        dati_principale = spark_to_pandas(calcolo_ritardo_per_mesi_compagnia(compagnia=compagnia_selezionata, mese_inizio=mese_inizio_comp, mese_fine=mese_fine_comp))
        dati_principale['Data'] = pd.to_datetime(dati_principale['Month'], format='%m').map(lambda d: d.replace(year=2013))
        dati_principale = dati_principale[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": compagnia_selezionata})
        dati_compagnie[compagnia_selezionata] = dati_principale

        for compagnia in confronto_comp:
            if compagnia != compagnia_selezionata:
                dati_confronto = spark_to_pandas(calcolo_ritardo_per_mesi_compagnia(compagnia=compagnia, mese_inizio=mese_inizio_comp, mese_fine=mese_fine_comp))
                dati_confronto['Data'] = pd.to_datetime(dati_confronto['Month'], format='%m').map(lambda d: d.replace(year=2013))
                dati_confronto = dati_confronto[['Data', 'ritardo_medio']].rename(columns={"ritardo_medio": compagnia})
                dati_compagnie[compagnia] = dati_confronto
    else:  
        with colonne_slider_comp[0]:
            data_inizio_comp = st.date_input("Seleziona la data di inizio", value=pd.to_datetime("2013-01-01"), format="DD-MM-YYYY", key="date_start_airline")
        with colonne_slider_comp[1]:
            data_fine_comp = st.date_input("Seleziona la data di fine", value=pd.to_datetime("2013-12-31"), format="DD-MM-YYYY", key="date_end_airline")

        dati_principale = spark_to_pandas(calcola_ritardo_giornaliero_compagnia(compagnia=compagnia_selezionata, data_inizio=str(data_inizio_comp), data_fine=str(data_fine_comp)))
        dati_principale['Data'] = pd.to_datetime(dati_principale['FlightDate'])
        dati_principale = dati_principale[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": compagnia_selezionata})
        dati_compagnie[compagnia_selezionata] = dati_principale
       
        for compagnia in confronto_comp:
            if compagnia != compagnia_selezionata:
                dati_confronto = spark_to_pandas(calcola_ritardo_giornaliero_compagnia(compagnia=compagnia, data_inizio=str(data_inizio_comp), data_fine=str(data_fine_comp)))
                dati_confronto['Data'] = pd.to_datetime(dati_confronto['FlightDate'])
                dati_confronto = dati_confronto[['Data', 'ritardo_medio_giornaliero']].rename(columns={"ritardo_medio_giornaliero": compagnia})
                dati_compagnie[compagnia] = dati_confronto
  
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
        totale_voli_anno = numero_voli_per_compagnia(compagnia_selezionata)
        media_giornaliera = totale_voli_anno / 365
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
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
    
    else: 
        dati_compagnie = {}
        cancellazioni_date_importanti = {}
        cancellazioni_anno = percentuale_voli_cancellati_compagnia(compagnia_selezionata)
        cancellazioni_date_importanti["Media Annuale"] = cancellazioni_anno
        
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
    coordinate_df=load_coordinate_data()
    tratte_coords = get_coordinates(tratte_percorse_compagnia, coordinate_df)
    disegna_tratta(tratte_coords, colonne_tab4[1])