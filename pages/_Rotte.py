import streamlit as st
from  queries import *
from datetime import datetime
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


st.title("Analisi delle Rotte Aeree")

data_inizio, data_fine = st.slider(
    "Seleziona il periodo di analisi",
    min_value=datetime(2013, 1, 1),  
    max_value=datetime(2013, 12, 31),  
    value=(datetime(2013, 1, 1), datetime(2013, 12, 31)), 
    format="YYYY-MM-DD"
)

col_stats, col_graph = st.columns([0.8, 2.2])

with col_stats:
    st.subheader("Statistiche delle Rotte")
    st.markdown("---")

    mese_piu_cancellati = spark_to_pandas(mese_con_piu_cancellati(data_inizio, data_fine))
    mese_piu = mese_piu_cancellati.iloc[0]["Month"]
    mese_piu_nome = mesi[mese_piu]
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Mese con più voli cancellati</div>
            <div class="stat-value">{mese_piu_nome}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
  
    mese_meno_cancellati = spark_to_pandas(mese_con_meno_cancellati(data_inizio, data_fine))
    mese_meno = mese_meno_cancellati.iloc[0]["Month"]
    mese_meno_nome = mesi[mese_meno]  
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Mese con meno voli cancellati</div>
            <div class="stat-value">{mese_meno_nome}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    aereo_piu_km = spark_to_pandas(aereo_piu_km_percorsi(data_inizio, data_fine))
    aereo = aereo_piu_km.iloc[0]["Tail_Number"]
    km_percorsi = aereo_piu_km.iloc[0]["TotalDistance"]
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Aereo con più miglia percorsi</div>
            <div class="stat-value">{aereo} - {km_percorsi} miglia</div>
        </div>
        """,
        unsafe_allow_html=True
    )
 
    numero_voli_recupero = voliRitardoDecolloArrivoAnticipo(data_inizio, data_fine)
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Voli con ritardo recuperato</div>
            <div class="stat-value">{numero_voli_recupero}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
   
    velocita_media = spark_to_pandas(velocita_media_totale(data_inizio, data_fine))
    velocita_media_valore = velocita_media.iloc[0]["AverageAircraftSpeed"]
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Velocità media totale</div>
            <div class="stat-value">{velocita_media_valore:.2f} miglia/h</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    
   
    percentuale_in_orario = percentualeVoliInOrario(data_inizio, data_fine)  
    percentuale_in_ritardo = percentuale_voli_ritardo_date(data_inizio, data_fine)
 
    st.markdown("**Voli In Orario**")
    st.altair_chart(make_donut(percentuale_in_orario, "In Orario", "green"), use_container_width=True)   
    st.markdown("**Voli in Ritardo**")
    st.altair_chart(make_donut(percentuale_in_ritardo, "In Ritardo", "red"), use_container_width=True)
    
    giorno_piu_voli = spark_to_pandas(giorno_della_settimana_con_piu_voli_date(data_inizio, data_fine))
    giorno_num = giorno_piu_voli.iloc[0]["DayOfWeek"]
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Giorno con più voli</div>
            <div class="stat-value">{converti_giorno(giorno_num)}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    max_dist = spark_to_pandas(volo_distanza_max(data_inizio, data_fine))
    min_dist = spark_to_pandas(volo_distanza_min(data_inizio, data_fine))
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Distanza Massima</div>
            <div class="stat-value">{max_dist.iloc[0]['Distance']} miglia</div>
        </div>
        <div class="stat-widget">
            <div class="stat-title">Distanza Minima</div>
            <div class="stat-value">{min_dist.iloc[0]['Distance']} miglia</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    totale_cancellazioni = totale_voli_cancellati_date(data_inizio, data_fine)
    
    st.markdown(
        f"""
        <div class="stat-widget">
            <div class="stat-title">Totale Voli Cancellati</div>
            <div class="stat-value">{totale_cancellazioni}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    
with col_graph:

    st.subheader("Voli per Mese")
  
    voli_per_mese = spark_to_pandas(totaleVoliPerMeseDate(data_inizio, data_fine))
    mesi = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }
    voli_per_mese["Month_Name"] = voli_per_mese["Month"].map(mesi)
    voli_per_mese = voli_per_mese.sort_values("Month")
    voli_per_mese["Month_Name"] = pd.Categorical(
        voli_per_mese["Month_Name"], categories=mesi_ordinati, ordered=True
    )
    voli_per_mese.set_index("Month_Name", inplace=True)

    st.bar_chart(voli_per_mese["NumeroVoli"])    
    st.markdown("---") 

    tabella1, tabella2, tabella3 = st.columns(3)
    
    # Tabella 1: Rotte più comuni
    with tabella1:
        st.markdown("#### Rotte più Comuni")
        rotte_comuni = spark_to_pandas(rottePiuComuni(data_inizio, data_fine))
        st.dataframe(rotte_comuni, use_container_width=True, hide_index=True,)
    
    # Tabella 2: Tratte con più ritardi
    with tabella2:
        st.markdown("#### Tratte con Più Ritardi")
        tratte_piu_ritardi = spark_to_pandas(tratte_con_piu_ritardi_totali(data_inizio, data_fine))
        st.dataframe(tratte_piu_ritardi, use_container_width=True, hide_index=True,)
    
    # Tabella 3: Tratte con meno ritardi
    with tabella3:
        st.markdown("#### Tratte con Meno Ritardi")
        tratte_meno_ritardi = spark_to_pandas(tratte_con_meno_ritardi_totali(data_inizio, data_fine))
        st.dataframe(tratte_meno_ritardi, use_container_width=False, hide_index=True,)

    st.markdown("### Visualizzazione delle Rotte per Aereo")

    aerei_disponibili = spark_to_pandas(get_aerei_disponibili(data_inizio.strftime("%Y-%m-%d"), data_fine.strftime("%Y-%m-%d")))
    selected_tail_number = st.selectbox("Seleziona un Aereo", aerei_disponibili)
    tratte_distinte = tratte_distinte_per_aereo(selected_tail_number, data_inizio.strftime("%Y-%m-%d"), data_fine.strftime("%Y-%m-%d"))  
    tratte_pd = spark_to_pandas(tratte_distinte)
    coordinate_df=load_coordinate_data()
    tratte_coords = get_coordinates(tratte_pd,coordinate_df)
    
    if not tratte_coords.empty:
        disegna_tratta(tratte_coords, col_graph)
    else:
        st.warning("Nessuna tratta trovata o coordinate mancanti.")

    st.markdown("### Selezione della Tratta per sapere la Velocità Media")
    
    originDest_col=st.columns(2)

    with originDest_col[0]:
        origin = st.selectbox("Seleziona Aeroporto di Partenza", tratte_pd['Origin'].unique())
    with originDest_col[1]:
        dest = st.selectbox("Seleziona Aeroporto di Destinazione", tratte_pd['Dest'].unique())
  
    if origin and dest:
        
        velocita_query_result = spark_to_pandas(velocita_media_per_tratta_specifica(origin, dest))
        if not velocita_query_result.empty:
            velocita_media_valore = velocita_query_result.iloc[0]["AverageSpeedForRoute"]
            if pd.notna(velocita_media_valore):
                
                st.markdown(
                    f"""
                    <div class="stat-widget">
                        <div class="stat-title">Velocità media per la tratta {origin} → {dest}</div>
                        <div class="stat-value">{velocita_media_valore:.2f} miglia/h</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"""
                    <div class="stat-widget">
                        <div class="stat-title">Velocità media per la tratta {origin} → {dest}</div>
                        <div class="stat-value">Tratta non percorsa da alcun aereo</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                f"""
                <div class="stat-widget">
                    <div class="stat-title">Velocità media per la tratta {origin} → {dest}</div>
                    <div class="stat-value">Tratta non percorsa da alcun aereo</div>
                </div>
                """,
                unsafe_allow_html=True
            )
    else:
        st.info("Seleziona partenza e destinazione per calcolare la velocità media.")
    
    st.markdown("### Analisi Cancellazioni")
    
    vista = st.segmented_control(
        "Tipo di analisi",
        ["Giorno della Settimana", "Causa Cancellazione"],selection_mode="single",default="Giorno della Settimana"
    )
    
    if vista == "Giorno della Settimana":
        cancellazioni_giorno = spark_to_pandas(cancellazioniPerGiorno(data_inizio, data_fine))
        
        giorni = {
            1: "Lunedì", 2: "Martedì", 3: "Mercoledì", 4: "Giovedì",
            5: "Venerdì", 6: "Sabato", 7: "Domenica"
        }
        
        cancellazioni_giorno["Giorno"] = cancellazioni_giorno["DayOfWeek"].map(giorni)
        
        st.bar_chart(
            data=cancellazioni_giorno,
            x="Giorno",
            y="Cancellazioni",
            height=400,
            horizontal=True
        )
    else:
        cancellazioni_causa = spark_to_pandas(cancellazioniPerCausa(data_inizio, data_fine))
        cause = {
            "A": "Compagnia Aerea",
            "B": "Meteo",
            "C": "Sistema Nazionale",
            "D": "Sicurezza"
        }
        cancellazioni_causa["Causa"] = cancellazioni_causa["CancellationCode"].map(cause)
        st.bar_chart(
            data=cancellazioni_causa,
            x="Causa",
            y="count",
            height=400,
            horizontal=True
        )