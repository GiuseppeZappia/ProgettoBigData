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


st.title("Analisi dei dati relativi ai singoli aeroporti")
aeroporto_selezionato = st.selectbox("Seleziona un aeroporto", load_airport_codes())
colonne_tab3 = st.columns((0.75,2), gap='medium')

with colonne_tab3[0]:
    
    st.subheader("Info Aeroporto",divider="grey")
    st.metric(label="Città dell'aeroporto",value=trova_citta_da_aeroporto(aeroporto_selezionato))
    st.metric(label="Voli totali passati per l'aeroporto", value=numero_voli_per_aeroporto(aeroporto_selezionato))
    
    giorno_piu_voli=spark_to_pandas(giorno_della_settimana_con_piu_voli(aeroporto_selezionato))
    giorno_num=giorno_piu_voli.iloc[0]["DayOfWeek"]
    numero_voli=giorno_piu_voli.iloc[0]["count"]

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
    
    perc_anticipo=percentuale_voli_orario(aeroporto_selezionato)
    in_anticipo=make_donut(perc_anticipo,"Voli in orario","green")
    
    st.altair_chart(in_anticipo)
    st.write('In ritardo')
    
    perc_ritardo=percentuale_voli_ritardo(aeroporto_selezionato)
    in_ritardo=make_donut(perc_ritardo,"Voli in ritardo","red")
    
    st.altair_chart(in_ritardo)
    
    st.markdown("#### Numero di voli per mese")
    
    voli_mesi_aeroporto=spark_to_pandas(totaleVoliPerMese(aeroporto_selezionato))
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
    st.subheader("Analisi Aeroporto",divider="red") 
    
    direzione=st.segmented_control("Direzione",["Da","Verso"],selection_mode="single",default="Da")
    mappa_opzione = option_menu(
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
        key='mappa_opzione_aeroporto'
    ) 
    
    if direzione=="Da":    
        if mappa_opzione=="Volo più distante":
            volo_distanza_max_da=spark_to_pandas(volo_distanza_max_da_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_distanza_max_da.iloc[0]["OriginCityName"]
            partenza_code=volo_distanza_max_da.iloc[0]["Origin"]
            citta_arrivo=volo_distanza_max_da.iloc[0]["DestCityName"]
            dest_code=volo_distanza_max_da.iloc[0]["Dest"]
            coordinate_df=load_coordinate_data()
            tratte_coords = get_coordinates(volo_distanza_max_da, coordinate_df)
            st.markdown(f"Tratta con distanza max partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_max_da.iloc[0]['Distance']} miglia")
            disegna_tratta(tratte_coords,colonne_tab3[1])
        
        elif mappa_opzione=="Volo più vicino":
            volo_distanza_min_da=spark_to_pandas(volo_distanza_min_da_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_distanza_min_da.iloc[0]["OriginCityName"]
            partenza_code=volo_distanza_min_da.iloc[0]["Origin"]
            citta_arrivo=volo_distanza_min_da.iloc[0]["DestCityName"]
            dest_code=volo_distanza_min_da.iloc[0]["Dest"]
            coordinate_df=load_coordinate_data()
            tratte_coords = get_coordinates(volo_distanza_min_da, coordinate_df)
            
            st.markdown(f"Tratta con distanza min partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_min_da.iloc[0]['Distance']} miglia")
            
            disegna_tratta(tratte_coords,colonne_tab3[1])
        
        elif mappa_opzione=="Tratta più percorsa":
            volo_piu_percorso_da=spark_to_pandas(tratta_piu_percorsa_da_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_piu_percorso_da.iloc[0]["OriginCityName"]
            partenza_code=volo_piu_percorso_da.iloc[0]["Origin"]
            citta_arrivo=volo_piu_percorso_da.iloc[0]["DestCityName"]
            dest_code=volo_piu_percorso_da.iloc[0]["Dest"]
            coordinate_df=load_coordinate_data()
            
            tratte_coords = get_coordinates(volo_piu_percorso_da, coordinate_df)
            
            st.markdown(f"Tratta più percorsa: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_da.iloc[0]['count']} volte ")
            
            disegna_tratta(tratte_coords,colonne_tab3[1])
    
    else:        
        if mappa_opzione=="Volo più distante":
            volo_distanza_max_per=spark_to_pandas(volo_distanza_max_verso_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_distanza_max_per.iloc[0]["OriginCityName"]
            partenza_code=volo_distanza_max_per.iloc[0]["Origin"]
            citta_arrivo=volo_distanza_max_per.iloc[0]["DestCityName"]
            dest_code=volo_distanza_max_per.iloc[0]["Dest"]
            coordinate_df=load_coordinate_data()
            tratte_coords = get_coordinates(volo_distanza_max_per, coordinate_df)
            
            st.markdown(f"Tratta con distanza max partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_max_per.iloc[0]['Distance']} miglia")
            
            disegna_tratta(tratte_coords,colonne_tab3[1])
        
        elif mappa_opzione=="Volo più vicino":
            volo_distanza_min_per=spark_to_pandas(volo_distanza_min_verso_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_distanza_min_per.iloc[0]["OriginCityName"]
            partenza_code=volo_distanza_min_per.iloc[0]["Origin"]
            citta_arrivo=volo_distanza_min_per.iloc[0]["DestCityName"]
            dest_code=volo_distanza_min_per.iloc[0]["Dest"]

            coordinate_df=load_coordinate_data()
            tratte_coords = get_coordinates(volo_distanza_min_per, coordinate_df)
            
            st.markdown(f"Tratta con distanza min partita da :red[{citta_partenza}] ({partenza_code}) e arrivata a :violet[{citta_arrivo}] ({dest_code}) percorrendo {volo_distanza_min_per.iloc[0]['Distance']} miglia")
            
            disegna_tratta(tratte_coords,colonne_tab3[1])

        elif mappa_opzione=="Tratta più percorsa":     
            volo_piu_percorso_per=spark_to_pandas(tratta_piu_percorsa_verso_aeroporto(aeroporto_selezionato))
            citta_partenza=volo_piu_percorso_per.iloc[0]["OriginCityName"]
            partenza_code=volo_piu_percorso_per.iloc[0]["Origin"]
            citta_arrivo=volo_piu_percorso_per.iloc[0]["DestCityName"]
            dest_code=volo_piu_percorso_per.iloc[0]["Dest"]

            coordinate_df=load_coordinate_data()
            tratte_coords = get_coordinates(volo_piu_percorso_per, coordinate_df)
           
            st.markdown(f"Tratta più percorsa: da :red[{citta_partenza}] ({partenza_code}) a :violet[{citta_arrivo}] ({dest_code}) percorsa {volo_piu_percorso_per.iloc[0]['count']} volte ")
            
            disegna_tratta(tratte_coords,colonne_tab3[1])
   
  
    st.subheader("Grafici", divider="red")
    st.markdown('#### Principali cause ritardo')

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
        confronto=st.multiselect("Aeroporto per confronto",load_airport_codes(),key='confronto_ritardi')#metto la key altrimenti streamlit da problemi se non lo distinguo da quello sotto che ha stessi parametri ma serve per vedere numero voli nelle date importanti
    colonne_slider=st.columns(2)
    dati_aeroporti = {}
    
    if mese_giornaliero=="Mensile":
        with colonne_slider[0]:
            mese_inizio = st.slider("Seleziona il mese di inizio", 1, 12, value=1)
        with colonne_slider[1]:
            mese_fine = st.slider("Seleziona il mese di fine", 1, 12, value=12)


        # #QUESTE DUE ISTRUZIONI SONO EQUIVALENTI E MI PERMETTONO DI MAPPARE I RITARDI MEDI SUI MESI CHE USERO' COME ASCISSE DEL GRAFICO
        # #IN PARTICOLARE CONSIDERANDO L'INTERO MESE MAPPO CON UNA DATA CHE PARTE DAL PRIMO DI QUEL MESE NELL'ANNO 2013
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
        menu_title=None, 
        options=["Voli partiti", "Voli atterrati", "Voli totali","Percentuale Cancellazioni"],  
        icons=["send", "map", "globe", "x-circle"], 
        default_index=0,  
        orientation="horizontal", 
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
    confronto_date_importanti=st.multiselect("Stato per confronto",load_airport_codes(),key='date_importanti')
    
    if grafico_a_barre_opzione == "Voli partiti":
        dati_aeroporti = {}    
        voli_date_importanti = {}
        totale_voli_anno = totale_voli_da_aeroporto(aeroporto_selezionato)
        media_giornaliera = totale_voli_anno / 365       
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
        for data in date_importanti:
            num_voli = totale_voli_da_aeroporto(aeroporto_selezionato, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli
        
        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', aeroporto_selezionato])
        df_principale.set_index('Data', inplace=True)
        dati_aeroporti[aeroporto_selezionato] = df_principale

        for aeroporto in confronto_date_importanti:
            if aeroporto != aeroporto_selezionato:
                voli_confronto = {}
                totale_voli_anno_confronto = totale_voli_da_aeroporto(aeroporto)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365                
                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto

                for data in date_importanti:
                    num_voli = totale_voli_da_aeroporto(aeroporto, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', aeroporto])
                df_confronto.set_index('Data', inplace=True)
                dati_aeroporti[aeroporto] = df_confronto

        df_combinato = pd.DataFrame()
        for aeroporto, dati in dati_aeroporti.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli partiti dall'aeroporto nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli per ogni aeroporto, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)        
        st.bar_chart(data=df_combinato, use_container_width=True)

    elif grafico_a_barre_opzione == "Voli atterrati":
        dati_aeroporti = {}
        voli_date_importanti = {}        
        totale_voli_anno = totale_voli_verso_aeroporto(aeroporto_selezionato)
        media_giornaliera = totale_voli_anno / 365
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
        for data in date_importanti:
            num_voli = totale_voli_verso_aeroporto(aeroporto_selezionato, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli
        
        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', aeroporto_selezionato])
        df_principale.set_index('Data', inplace=True)
        dati_aeroporti[aeroporto_selezionato] = df_principale

        for aeroporto in confronto_date_importanti:
            if aeroporto != aeroporto_selezionato:
                voli_confronto = {}
                totale_voli_anno_confronto = totale_voli_verso_aeroporto(aeroporto)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365
                
                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                
                for data in date_importanti:
                    num_voli = totale_voli_verso_aeroporto(aeroporto, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', aeroporto])
                df_confronto.set_index('Data', inplace=True)
                dati_aeroporti[aeroporto] = df_confronto

        df_combinato = pd.DataFrame()
        for aeroporto, dati in dati_aeroporti.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli arrivati nell'aeroporto nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli per ogni aeroporto, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)        
        st.bar_chart(data=df_combinato, use_container_width=True)
    
    elif grafico_a_barre_opzione == "Voli totali":
        dati_aeroporti = {}            
        voli_date_importanti = {}
        
        totale_voli_anno = numero_voli_per_aeroporto(aeroporto_selezionato)
        media_giornaliera = totale_voli_anno / 365
        
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
        for data in date_importanti:
            num_voli = numero_voli_per_aeroporto(aeroporto_selezionato, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli
        
        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', aeroporto_selezionato])
        df_principale.set_index('Data', inplace=True)
        dati_aeroporti[aeroporto_selezionato] = df_principale

        for aeroporto in confronto_date_importanti:
            if aeroporto != aeroporto_selezionato:
                voli_confronto = {}
                totale_voli_anno_confronto = numero_voli_per_aeroporto(aeroporto)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365
                
                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                
                for data in date_importanti:
                    num_voli = numero_voli_per_aeroporto(aeroporto, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', aeroporto])
                df_confronto.set_index('Data', inplace=True)
                dati_aeroporti[aeroporto] = df_confronto
        df_combinato = pd.DataFrame()
        for aeroporto, dati in dati_aeroporti.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli totali passati per l'aeroporto nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli totali per ogni aeroporto, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)        
        st.bar_chart(data=df_combinato, use_container_width=True)
    
    elif grafico_a_barre_opzione == "Percentuale Cancellazioni":
        dati_aeroporti = {}
        cancellazioni_date_importanti = {}
        cancellazioni_anno = percentuale_voli_cancellati_aeroporto(aeroporto_selezionato)
        cancellazioni_date_importanti["Media Annuale"] = cancellazioni_anno
        
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

    coordinate_df=load_coordinate_data()
    tratte_coords = get_coordinates(aeroporti_collegati, coordinate_df)
    disegna_tratta(tratte_coords, colonne_tab3[1])