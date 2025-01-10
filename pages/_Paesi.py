import plotly.express as px
import pandas as pd
from queries import *
import streamlit as st
from datetime import datetime
from streamlit_option_menu import option_menu
from utils import *
import calendar


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

st.title("Analisi dei dati relativi ai paesi")   

col_st1, col_st2 = st.columns([0.8, 2.2])

with col_st1:
    di1, df1 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  
        max_value=datetime(2013, 12, 31),  
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  
        format="YYYY-MM-DD",
        key="slider_stati_piu_visitati"  
    )

    risultati = stato_piu_visitato_date(di1, df1)
    stato_df = spark_to_pandas(risultati["stato_piu_visitato"])
    stato = stato_df.iloc[0]["DestStateName"]
    totale_visite = stato_df.iloc[0]["TotaleVisite"]

    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Stato più visitato</div>
            <div class="stat-value">{stato}</div>
            <div class="tooltip">
                <strong>Totale visite:</strong> {totale_visite}<br>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    di2, df2 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  
        max_value=datetime(2013, 12, 31),  
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  
        format="YYYY-MM-DD",
        key="slider_ritardi_stati"  
    )


    ritardi_df = spark_to_pandas(ritardo_medio_per_stato(di2, df2))
    stato_top = ritardi_df.iloc[0]["OriginStateName"]
    ritardo_top = ritardi_df.iloc[0]["RitardoMedio"]
    mostra_tabella = st.checkbox("Mostra tabella dettagliata")

    st.markdown(
        f"""
        <div class="stat-widget" onclick="document.getElementById('dettagli-ritardi').style.display='block'">
            <div class="stat-title">Stato con il maggiore ritardo medio alla partenza</div>
            <div class="stat-value">{stato_top}</div>
            <div class="stat-title">Ritardo medio: {ritardo_top:.2f} minuti</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    if mostra_tabella:
        st.markdown("### Ritardo medio alla partenza per ogni stato di origine")
        st.dataframe(ritardi_df, use_container_width=True, hide_index=True)

    di3, df3 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_voli_stati"  # Identificatore univoco per lo slider
    )
    
    voli_df = spark_to_pandas(voli_per_stato_origine(di3, df3))

    stato_top_voli = voli_df.iloc[0]["OriginStateName"]
    totale_top_voli = voli_df.iloc[0]["TotaleVoli"]

    mostra_tabella_voli = st.checkbox("Mostra tabella dettagliata", key="checkbox_voli_stati")

    st.markdown(
        f"""
        <div class="stat-widget" onclick="document.getElementById('dettagli-voli').style.display='block'">
            <div class="stat-title">Stato con il maggiore numero di voli partiti</div>
            <div class="stat-value">{stato_top_voli}</div>
            <div class="stat-title">Totale voli: {totale_top_voli}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    # Tabella dettagliata (visibile con il checkbox)
    if mostra_tabella_voli:
        st.markdown("### Numero totale di voli per ogni stato di origine")
        st.dataframe(voli_df, use_container_width=True, hide_index=True)
    # Slider per la selezione del periodo
    di4, df4 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_rapporto_voli"  # Identificatore univoco per lo slider
    )
    # Selezione dello stato
    stati_disponibili = spark_to_pandas(stati_distinti())
    stato_selezionato = st.selectbox(
        "Seleziona uno stato per visualizzare il rapporto specifico (opzionale)",
        options=["Tutti"] + stati_disponibili["OriginStateName"].tolist(),
        key="select_stato_rapporto"
    )
    # Calcola il rapporto
    stato_param = None if stato_selezionato == "Tutti" else stato_selezionato
    rapporto_df = spark_to_pandas(rapporto_voli_cancellati(di4, df4, stato_param))
    # Mostra il risultato principale
    if stato_param:
        stato_top = rapporto_df.iloc[0]["OriginStateName"]
        rapporto_top = rapporto_df.iloc[0]["RapportoCancellati"]
        totale_voli_top = rapporto_df.iloc[0]["TotaleVoli"]
        voli_cancellati_top = rapporto_df.iloc[0]["VoliCancellati"]
        st.markdown(
            f"""
            <div class="stat-widget">
                <div class="stat-title">Rapporto cancellazioni per {stato_top}</div>
                <div class="stat-value">{rapporto_top:.2%}</div>
                <div class="stat-title">Totale voli: {totale_voli_top}, Cancellati: {voli_cancellati_top}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        # Rimuovi la colonna del rapporto e mostra la tabella senza indice
        rapporto_df = rapporto_df.drop(columns=["RapportoCancellati"])
        st.markdown("### Dettaglio voli per lo stato selezionato")
        st.dataframe(rapporto_df.reset_index(drop=True), use_container_width=True, hide_index=True)
    else:
        st.markdown(
            """
            <div class="stat-widget">
                <div class="stat-title">Rapporto cancellazioni per tutti gli stati</div>
                <div class="stat-value">Tabella sotto</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        # Mostra la tabella completa senza indice
        rapporto_df = rapporto_df.drop(columns=["RapportoCancellati"])
        st.markdown("### Rapporto tra voli cancellati e voli totali per ogni stato")
        st.dataframe(rapporto_df.reset_index(drop=True), use_container_width=True, hide_index=True)
    # Slider per la selezione del periodo
    di5, df5 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_tempi_volo"  # Identificatore univoco per lo slider
    )
    # Selezione dello stato
    stati_disponibili = spark_to_pandas(stati_distinti())
    stato_selezionato = st.selectbox(
        "Seleziona uno stato per visualizzare i tempi di volo (opzionale)",
        options=["Tutti"] + stati_disponibili["OriginStateName"].tolist(),
        key="select_stato_tempi"
    )
    # Calcola i voli più veloci e più lenti
    stato_param = None if stato_selezionato == "Tutti" else stato_selezionato
    risultati_tempi = tempi_volo_per_stato(di5, df5, stato_param)
    # Converti i risultati in Pandas DataFrame
    volo_piu_veloce = spark_to_pandas(risultati_tempi["volo_piu_veloce"])
    volo_piu_lento = spark_to_pandas(risultati_tempi["volo_piu_lento"])
    # Mostra i widget principali
    if not volo_piu_veloce.empty:
        stato_veloce = volo_piu_veloce.iloc[0]["OriginStateName"]
        tempo_veloce = volo_piu_veloce.iloc[0]["AirTime"]
        destinazione_veloce = volo_piu_veloce.iloc[0]["DestStateName"]
        st.markdown(
            f"""
            <div class="stat-widget">
                <div class="stat-title">Volo più veloce</div>
                <div class="stat-value">{tempo_veloce} minuti</div>
                <div class="stat-title">Da {stato_veloce} a {destinazione_veloce}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    if not volo_piu_lento.empty:
        stato_lento = volo_piu_lento.iloc[0]["OriginStateName"]
        tempo_lento = volo_piu_lento.iloc[0]["AirTime"]
        destinazione_lento = volo_piu_lento.iloc[0]["DestStateName"]
        st.markdown(
            f"""
            <div class="stat-widget">
                <div class="stat-title">Volo più lento</div>
                <div class="stat-value">{tempo_lento} minuti</div>
                <div class="stat-title">Da {stato_lento} a {destinazione_lento}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
# Colonna destra - Grafici
with col_st2:
    st.markdown("### Ritardi medi per stato nel giorno selezionato")
    # Slider per la selezione del periodo
    di6, df6 = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_ritardi_giorno"  # Identificatore univoco per lo slider
    )
    st.markdown("""
        Il grafico mostra il ritardo medio nei vari stati in un giorno specifico della settimana selezionabile dall'utente.
        """)
    # Creazione del menu orizzontale con option_menu
    giorno_selezionato = option_menu(
        menu_title=None,  # Nessun titolo
        options=["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"],  # Giorni della settimana
        icons=["calendar-day"] * 7,  # Icone (una per ogni opzione)
        default_index=0,  # Giorno di default (Lunedì)
        orientation="horizontal",  # Menu orizzontale
        styles={
            "container": {"padding": "0px", "background-color": "#f8f9fa"},
            "icon": {"color": "light-grey", "font-size": "18px"},
            "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
            "nav-link-selected": {"background-color": "light-grey"},
        },
    )
    # Mappatura del giorno selezionato in formato numerico
    giorni_map = {
        "Lunedì": 1,
        "Martedì": 2,
        "Mercoledì": 3,
        "Giovedì": 4,
        "Venerdì": 5,
        "Sabato": 6,
        "Domenica": 7,
    }
    giorno_valore = giorni_map[giorno_selezionato]
    # Calcola i ritardi medi per stato in base al giorno selezionato
    ritardi_df = spark_to_pandas(ritardi_medi_per_giorno(di6, df6, giorno_valore))
    # Grafico a barre
    
    st.bar_chart(
        data=ritardi_df.set_index("OriginStateName")["RitardoMedio"],
        use_container_width=True
    )
    st.markdown("### Ritardi medi nei mesi invernali per causa")
    st.markdown("""Il grafico mostra il ritardo medio nei mesi invernali per ogni stato, è possibile scegliere una causa specifica e visualizzarne gli effetti""")
    # Bottoni per selezionare la causa
    causa_selezionata = option_menu(
        menu_title=None,  # Nessun titolo
        options=["Tutti", "Compagnia Aerea", "Meteo", "Sistema Nazionale", "Sicurezza"],  # Opzioni per la causa
        icons=["filter", "airplane", "cloud", "network-wired", "shield-alt"],  # Icone
        default_index=0,  # Indice predefinito (Tutti)
        orientation="horizontal",  # Menu orizzontale
        styles={
            "container": {"padding": "0px", "background-color": "#f8f9fa"},
            "icon": {"color": "light-grey", "font-size": "18px"},
            "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
            "nav-link-selected": {"background-color": "light-grey"},
        },
    )
    
    # Mappatura della causa selezionata ai codici del dataset
    causa_map = {
        "Tutti": None,
        "Compagnia Aerea": "A",
        "Meteo": "B",
        "Sistema Nazionale": "C",
        "Sicurezza": "D",
    }
    causa_valore = causa_map[causa_selezionata]
    # Calcola i ritardi medi per stato
    ritardi_invernali_df = spark_to_pandas(incremento_ritardi_invernali(causa_valore))
    # Grafico a barre
    
    st.bar_chart(
        data=ritardi_invernali_df.set_index("OriginStateName")["RitardoMedio"],
        use_container_width=True
    )

    # Slider per la selezione del periodo
    diaaa, dfaaa = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_analisi_stati"
    )
    # Esegui le query
    ritardi_df = spark_to_pandas(stati_con_minor_ritardo_date(diaaa, dfaaa))
    visitati_df = spark_to_pandas(stati_piu_visitati_date(diaaa, dfaaa))
    traffico_df = spark_to_pandas(stati_maggiore_traffico_date(diaaa, dfaaa))
    # cancellazioni_df = spark_to_pandas(stati_percentuale_voli_cancellati(diaaa, dfaaa))
    maggiore_increm=spark_to_pandas(stati_con_maggiore_increm_ritardo_inverno_rispetto_estate(diaaa,dfaaa))
    st.markdown("### Tabelle")
    st.write("""
                - Minor Ritardo Medio 
                - Stati più Visitati 
                - Maggiore Traffico Aereo 
                - Incremento Ritardi invernali rispetto estate in minuti""")
    # Layout con quattro colonne
    col1, col2, col3, col4 = st.columns(4)
    # Tabella 6: Stati con il minor ritardo medio
    with col1:
        st.dataframe(
            ritardi_df.head(10).reset_index(drop=True),
            use_container_width=True,
            hide_index=True
        )
    # Tabella 7: Stati con il maggior numero di voli come destinazione
    with col2:
        st.dataframe(
            visitati_df.head(10).reset_index(drop=True),
            use_container_width=True,
            hide_index=True
        )
    # Tabella 8: Stati con il maggiore traffico aereo
    with col3:
        st.dataframe(
            traffico_df.head(10).reset_index(drop=True),
            use_container_width=True,
            hide_index=True
        )
    # Tabella 9: Stati con maggiore incremento dei ritardi in inverno rispetto estate
    with col4:
        st.dataframe(
            maggiore_increm[["OriginStateName", "IncrementoRitardi"]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True
        )
    st.subheader("Analisi delle percentuali di cause di cancellazioni")
    st.markdown("""Il grafico mostra la percentuale delle cause di cancellazione, è possibile scegliere un periodo specifico e visualizzarne le percentuali""")
    # Slider per la selezione del periodo
    dicc, dfcc = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_percentuali_cause"
    )
    # Calcola le percentuali delle cause
    cause_df = spark_to_pandas(percentuali_cause_cancellazioni(dicc, dfcc))
    # Mappatura delle cause
    cause_map = {
        "A": "Compagnia Aerea",
        "B": "Meteo",
        "C": "Sistema Nazionale",
        "D": "Sicurezza",
        None: "Altro"
    }
    cause_df["Causa"] = cause_df["CancellationCode"].map(cause_map)
    # Grafico a torta con Plotly
    fig = px.pie(
        cause_df,
        names="Causa",
        values="Percentuale",
        title="Percentuali delle cause di cancellazioni",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    # Mostra il grafico
    st.plotly_chart(fig, use_container_width=True)
    st.subheader("Analisi dell'efficienza degli stati (ritardi minimi rispetto al volume di voli)")
    st.markdown("""In questa sezione è graficata l'efficienza dei singoli stati, intesa come ritardo medio su numero di voli effettuati """)   
    # Slider per selezionare il periodo
    dies, dfes = st.slider(
        "",
        min_value=datetime(2013, 1, 1),  # Data minima del dataset
        max_value=datetime(2013, 12, 31),  # Data massima del dataset
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),  # Intervallo di default
        format="YYYY-MM-DD",
        key="slider_efficienza_stati"
    )
    # Query per calcolare l'efficienza
    stati_efficienza_df = spark_to_pandas(stati_efficienza(dies, dfes))
    # Verifica se ci sono dati
    if stati_efficienza_df.empty:
        st.warning("Non ci sono dati disponibili per il periodo selezionato.")
    else:
        # Prepara i dati per il grafico
        stati_efficienza_df = stati_efficienza_df.rename(columns={"OriginStateName": "Stato", "Efficienza": "IndiceEfficienza"})
        stati_efficienza_df = stati_efficienza_df.sort_values("IndiceEfficienza")
        # Grafico a linee con Streamlit
        st.line_chart(
            data=stati_efficienza_df.set_index("Stato")["IndiceEfficienza"],
            use_container_width=True,
            height=400
        )


    stato_selezionato_giorni=st.selectbox("Seleziona uno stato",options=stati_disponibili["OriginStateName"].dropna().tolist(),)
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
    confronto_date_importanti=st.multiselect("Aeroporto per confronto",load_airport_codes(),key='date_importanti')
    
    if grafico_a_barre_opzione == "Voli partiti":

        dati_stati = {}
            
        voli_date_importanti = {}

        totale_voli_anno = totale_voli_da_stato(stato_selezionato_giorni)
        media_giornaliera = totale_voli_anno / 365
        
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
        for data in date_importanti:
            num_voli = totale_voli_da_stato(stato_selezionato_giorni, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli
        
        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', stato_selezionato_giorni])
        df_principale.set_index('Data', inplace=True)
        dati_stati[stato_selezionato_giorni] = df_principale

        for stato in confronto_date_importanti:
            if stato != stato_selezionato_giorni:
                voli_confronto = {}
                totale_voli_anno_confronto = totale_voli_da_stato(stato_selezionato_giorni)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365
                
                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto

                for data in date_importanti:
                    num_voli = totale_voli_da_stato(stato_selezionato_giorni, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', stato])
                df_confronto.set_index('Data', inplace=True)
                dati_stati[stato] = df_confronto

        df_combinato = pd.DataFrame()
        for stato, dati in dati_stati.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli partiti dallo stato nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli per ogni stato, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)
        

        st.bar_chart(data=df_combinato, use_container_width=True)
    elif grafico_a_barre_opzione == "Voli atterrati":

        dati_stati = {}
            
        voli_date_importanti = {}
        
        totale_voli_anno = totale_voli_verso_stato(stato_selezionato_giorni)
        media_giornaliera = totale_voli_anno / 365
        
        voli_date_importanti["Media Giornaliera"] = media_giornaliera
        
        for data in date_importanti:
            num_voli = totale_voli_verso_stato(stato_selezionato_giorni, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli
        
        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', stato_selezionato_giorni])
        df_principale.set_index('Data', inplace=True)
        dati_stati[stato_selezionato_giorni] = df_principale

        for stato in confronto_date_importanti:
            if stato != stato_selezionato_giorni:
                voli_confronto = {}

                totale_voli_anno_confronto = totale_voli_verso_stato(stato_selezionato_giorni)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365
                

                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto
                
                for data in date_importanti:
                    num_voli = totale_voli_verso_stato(stato_selezionato_giorni, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', stato])
                df_confronto.set_index('Data', inplace=True)
                dati_stati[stato] = df_confronto

        df_combinato = pd.DataFrame()
        for stato, dati in dati_stati.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli arrivati nello stato nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli per ogni stato, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)
        

        st.bar_chart(data=df_combinato, use_container_width=True)
    elif grafico_a_barre_opzione == "Voli totali":

        dati_stati = {}   

        voli_date_importanti = {}
        
        totale_voli_anno = numero_voli_per_stato(stato_selezionato_giorni)
        media_giornaliera = totale_voli_anno / 365

        voli_date_importanti["Media Giornaliera"] = media_giornaliera

        for data in date_importanti:
            num_voli = numero_voli_per_stato(stato_selezionato_giorni, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            voli_date_importanti[data_formattata] = num_voli

        df_principale = pd.DataFrame(list(voli_date_importanti.items()), 
                                columns=['Data', stato_selezionato_giorni])
        df_principale.set_index('Data', inplace=True)
        dati_stati[stato_selezionato_giorni] = df_principale

        for stato in confronto_date_importanti:
            if stato != stato_selezionato_giorni:
                voli_confronto = {}

                totale_voli_anno_confronto = numero_voli_per_stato(stato_selezionato_giorni)
                media_giornaliera_confronto = totale_voli_anno_confronto / 365

                voli_confronto["Media Giornaliera"] = media_giornaliera_confronto

                for data in date_importanti:
                    num_voli = numero_voli_per_stato(stato, data)
                    data_dt = pd.to_datetime(data)
                    data_formattata = data_dt.strftime('%d %B')
                    voli_confronto[data_formattata] = num_voli
                
                df_confronto = pd.DataFrame(list(voli_confronto.items()),
                                        columns=['Data', stato])
                df_confronto.set_index('Data', inplace=True)
                dati_stati[stato] = df_confronto

        df_combinato = pd.DataFrame()
        for stato, dati in dati_stati.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)

        st.markdown("""
        Il grafico mostra il numero di voli totali passati per lo stato nelle date principali dell'anno.
        L'ultima barra rappresenta la media giornaliera dei voli totali per ogni stato, permettendo un confronto 
        immediato tra l'operatività normale e quella nei giorni speciali.
        """)

        st.bar_chart(data=df_combinato, use_container_width=True)
    elif grafico_a_barre_opzione == "Percentuale Cancellazioni":
        dati_stati = {}
        cancellazioni_date_importanti = {}

        cancellazioni_anno = percentuale_voli_cancellati_stato(stato_selezionato_giorni)
        cancellazioni_date_importanti["Media Annuale"] = cancellazioni_anno

        for data in date_importanti:
            cancellazioni = percentuale_voli_cancellati_stato(stato_selezionato_giorni, data)
            data_dt = pd.to_datetime(data)
            data_formattata = data_dt.strftime('%d %B')
            cancellazioni_date_importanti[data_formattata] = cancellazioni
        
        df_principale = pd.DataFrame(
            list(cancellazioni_date_importanti.items()), 
            columns=['Data', stato_selezionato_giorni]
        )
        df_principale.set_index('Data', inplace=True)
        dati_stati[stato_selezionato_giorni] = df_principale

        for stato in confronto_date_importanti:
            cancellazioni_confronto = {}
            
            df_cancellazioni_anno = percentuale_voli_cancellati_stato(stato)
            
            cancellazioni_confronto["Media Annuale"] = df_cancellazioni_anno
            
            for data in date_importanti:
                cancellazioni = percentuale_voli_cancellati_stato(stato, data)
                data_dt = pd.to_datetime(data)
                data_formattata = data_dt.strftime('%d %B')
                cancellazioni_confronto[data_formattata] = cancellazioni
            
            df_confronto = pd.DataFrame(
                list(cancellazioni_confronto.items()),
                columns=['Data', stato]
            )
            df_confronto.set_index('Data', inplace=True)
            dati_stati[stato] = df_confronto

        df_combinato = pd.DataFrame()
        for stato, dati in dati_stati.items():
            if df_combinato.empty:
                df_combinato = dati
            else:
                df_combinato = df_combinato.join(dati)
        st.markdown("""
        Il grafico mostra la percentuale di voli cancellati nell'stato nelle date principali dell'anno.
        L'ultima barra rappresenta la media annuale delle cancellazioni, permettendo un confronto 
        tra le cancellazioni normali e quelle nei giorni speciali.
        """)
        
        st.bar_chart(data=df_combinato, use_container_width=True)


    st.subheader("Stagionalità dei voli per stato specifico")

    stati_disponibili = spark_to_pandas(stati_distinti()).sort_values("OriginStateName")
    stato_selezionato = st.selectbox(
        "Seleziona uno stato",
        options=stati_disponibili["OriginStateName"].dropna().tolist(),
        index=0  # Seleziona il primo elemento come default
    )
    # Slider per selezionare il periodo
    data_inizio, data_fine = st.slider(
        "",
        min_value=datetime(2013, 1, 1),
        max_value=datetime(2013, 12, 31),
        value=(datetime(2013, 1, 1), datetime(2013, 12, 31)),
        format="YYYY-MM-DD"
    )
    # Esegui la query per calcolare la stagionalità
    stagionalita_df = spark_to_pandas(stagionalita_voli_per_stato(
        data_inizio=data_inizio.strftime("%Y-%m-%d"),
        data_fine=data_fine.strftime("%Y-%m-%d"),
        stato_selezionato=stato_selezionato
    ))
    # Verifica se ci sono dati
    if stagionalita_df.empty:
        st.warning(f"Nessun dato trovato per lo stato selezionato: {stato_selezionato}.")
    else:
        # Prepara i dati per il grafico
        stagionalita_df["Mese"] = stagionalita_df["Month"].apply(lambda x: calendar.month_name[x])
        stagionalita_df = stagionalita_df.rename(columns={"NumeroVoli": "Voli"})
        # Ordina i mesi nell'ordine corretto
        mesi_ordinati = list(calendar.month_name)[1:]  # Lista di mesi (esclude elemento vuoto iniziale)
        stagionalita_df["Mese"] = pd.Categorical(stagionalita_df["Mese"], categories=mesi_ordinati, ordered=True)
        stagionalita_df = stagionalita_df.sort_values("Mese")
        # Grafico a linee con Streamlit
        st.line_chart(
            data=stagionalita_df.set_index("Mese")["Voli"],
            use_container_width=True,
            height=400
        )