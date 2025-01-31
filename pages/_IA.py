import streamlit as st
from queries import *
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler as SklearnScaler
import plotly.express as px


def crea_overview_cluster(clusters_df):
    # Preparo i dati per PCA
    features = ["voli_totali", "ritardo_medio", "distanza_media", "rate_cancellazione"]
    X = clusters_df[features].values
    
    # Standardizzo i dati
    scaler = SklearnScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Applica PCA
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    # Crea DataFrame per la visualizzazione
    vis_df = pd.DataFrame(
        X_pca, 
        columns=['x', 'y']
    )
    vis_df['Cluster'] = clusters_df['cluster']
    vis_df['Aeroporto'] = clusters_df['airport_code']
    vis_df['Voli Totali'] = clusters_df['voli_totali']
    vis_df['Ritardo Medio'] = clusters_df['ritardo_medio']
    vis_df['Distanza Media'] = clusters_df['distanza_media']
    vis_df['Tasso Cancellazioni'] = clusters_df['rate_cancellazione']
    
    fig = px.scatter(
        vis_df,
        x='x',
        y='y',
        color='Cluster',
        hover_data=['Aeroporto', 'Voli Totali', 'Ritardo Medio', 
                   'Distanza Media', 'Tasso Cancellazioni'],
    )

    return fig

def crea_overview_rotte(clusters_df):
    features = ["voli_totali", "distanza", "ritardo_medio","varianza_ritardo","rate_cancellazione","rate_dirottamento","media_tempo_volo"]
    X = clusters_df[features].values
    
    # Standardizza i dati
    scaler = SklearnScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Applica PCA
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)

    vis_df = pd.DataFrame(
        X_pca, 
        columns=['x', 'y']
    )
    vis_df['Cluster'] = clusters_df['cluster']
    vis_df['Origine'] = clusters_df['Origin'] + " (" + clusters_df['OriginCityName'] + ")"
    vis_df['Destinazione'] = clusters_df['Dest'] + " (" + clusters_df['DestCityName'] + ")"
    vis_df['Voli Totali'] = clusters_df['voli_totali']
    vis_df['Distanza'] = clusters_df['distanza']
    vis_df['Ritardo Medio'] = clusters_df['ritardo_medio']

    fig = px.scatter(
        vis_df,
        x='x',
        y='y',
        color='Cluster',
        hover_data=['Origine', 'Destinazione', 'Voli Totali', 'Distanza', 'Ritardo Medio'],
    )
    
    return fig

    
st.title("Algoritmi di Machine Learning applicati ai dati")

scelta=st.radio("Seleziona il tipo di analisi",[":blue[Clustering degli aeroporti]",":blue[Clustering delle rotte]"],index=0,horizontal=True)

if scelta==":blue[Clustering degli aeroporti]":
    st.subheader("Analisi Cluster degli Aeroporti",divider="red")
    st.write("""
    Questa analisi raggruppa gli aeroporti in cluster basati su:
    - Volume totale di voli
    - Ritardo medio
    - Distanza media dei voli
    - Tasso di cancellazione
    """)

    num_clus = st.slider("Seleziona il numero di cluster", 2, 5, 4)
    
    try:
        clusters_df = cluster_airports(num_clus)
        
        st.subheader("Statistiche dei Cluster")
        
        cluster_stats = clusters_df.groupby("cluster").agg({
            "voli_totali": "mean",
            "ritardo_medio": "mean",
            "distanza_media": "mean",
            "rate_cancellazione": "mean",
            "airport_code": "count"
        }).round(2)
        
        cluster_stats.columns = [
            "Media Voli Totali",
            "Media Ritardo (min)",
            "Distanza Media (miglia)",
            "Tasso Cancellazione",
            "Numero Aeroporti"
        ]
        st.dataframe(cluster_stats)

        tab1, tab2 = st.tabs(["Panoramica Generale", "Analisi Dettagliata"])
        
        with tab1:
            st.subheader("Panoramica Generale dei Cluster")
            fig_overview = crea_overview_cluster(clusters_df)
            st.plotly_chart(fig_overview, use_container_width=True)
            st.write("""
            Questo grafico mostra una visione d'insieme dei cluster, dove:
            - Ogni punto rappresenta un aeroporto
            - La posizione è determinata da tutte le caratteristiche considerate
            - Aeroporti vicini hanno caratteristiche simili
            - I colori indicano i diversi cluster
            """)
        
        with tab2:
            
            st.subheader("Analisi Dettagliata")
            
            plot_type = st.selectbox(
                "Seleziona dimensioni da visualizzare:",
                ["Voli vs Ritardi", "Voli vs Distanza", "Ritardi vs Cancellazioni"]
            )
            
            if plot_type == "Voli vs Ritardi":
                fig = px.scatter(
                    clusters_df,
                    x="voli_totali",
                    y="ritardo_medio",
                    color="cluster",
                    hover_data=["airport_code"],
                    title="Cluster Aeroporti: Volume Voli vs Ritardo Medio"
                )
            elif plot_type == "Voli vs Distanza":
                fig = px.scatter(
                    clusters_df,
                    x="voli_totali",
                    y="distanza_media",
                    color="cluster",
                    hover_data=["airport_code"],
                    title="Cluster Aeroporti: Volume Voli vs Distanza Media"
                )
            else:
                fig = px.scatter(
                    clusters_df,
                    x="ritardo_medio",
                    y="rate_cancellazione",
                    color="cluster",
                    hover_data=["airport_code"],
                    title="Cluster Aeroporti: Ritardo Medio vs Tasso Cancellazioni"
                )
            
            st.plotly_chart(fig)

        st.subheader("Aeroporti per Cluster")
        
        selected_cluster = st.selectbox(
            "Seleziona un cluster da visualizzare:",
            sorted(clusters_df["cluster"].unique())
        )
        
        cluster_airports = clusters_df[clusters_df["cluster"] == selected_cluster].sort_values("voli_totali", ascending=False)
        
        st.dataframe(cluster_airports, hide_index=True) 
    
    except Exception as e:
        st.error(f"Errore nell'analisi: {str(e)}")

else:
    """Mostra l'interfaccia Streamlit per l'analisi dei cluster delle rotte"""
    
    st.title("Analisi Cluster delle Rotte Aeree")
    
    st.write("""
    Questa analisi raggruppa le rotte aeree in cluster basati su:
    - Volume totale di voli
    - Distanza della rotta
    - Ritardi medi e loro variabilità
    - Tasso di cancellazioni e dirottamenti
    - Tempo di volo medio
    """)
    
    num_clus = st.slider("Seleziona il numero di cluster", 2, 7, 5)
    try:
        clusters_df = cluster_routes(num_clus)
        
        # Mostra statistiche dei cluster
        st.subheader("Statistiche dei Cluster")
        cluster_stats = clusters_df.groupby("cluster").agg({
            "voli_totali": "mean",
            "distanza": "mean",
            "ritardo_medio": "mean",
            "varianza_ritardo": "mean",
            "rate_cancellazione": "mean",
            "Origin": "count" 
        }).round(2)
        
        cluster_stats.columns = [
            "Media Voli per Rotta",
            "Distanza Media (miglia)",
            "Ritardo Medio (min)",
            "Variabilità Ritardi",
            "Tasso Cancellazione",
            "Numero Rotte"
        ]
        st.dataframe(cluster_stats)
        
        tab1, tab2 = st.tabs(["Panoramica Generale", "Analisi Dettagliata"])
        
        with tab1:
            st.subheader("Panoramica Generale dei Cluster")
            fig_overview = crea_overview_rotte(clusters_df)
            st.plotly_chart(fig_overview, use_container_width=True)
            
        with tab2:
            st.subheader("Analisi Dettagliata")
            plot_type = st.selectbox(
                "Seleziona dimensioni da visualizzare:",
                ["Voli vs Distanza", "Voli vs Ritardi", "Distanza vs Ritardi"]
            )
            
            if plot_type == "Voli vs Distanza":
                fig = px.scatter(
                    clusters_df,
                    x="voli_totali",
                    y="distanza",
                    color="cluster",
                    hover_data=["Origin", "Dest", "OriginCityName", "DestCityName"],
                    title="Cluster Rotte: Volume Voli vs Distanza"
                )
            elif plot_type == "Voli vs Ritardi":
                fig = px.scatter(
                    clusters_df,
                    x="voli_totali",
                    y="ritardo_medio",
                    color="cluster",
                    hover_data=["Origin", "Dest", "OriginCityName", "DestCityName"],
                    title="Cluster Rotte: Volume Voli vs Ritardo Medio"
                )
            else:
                fig = px.scatter(
                    clusters_df,
                    x="distanza",
                    y="ritardo_medio",
                    color="cluster",
                    hover_data=["Origin", "Dest", "OriginCityName", "DestCityName"],
                    title="Cluster Rotte: Distanza vs Ritardo Medio"
                )
            
            st.plotly_chart(fig)
        
        # Lista rotte per cluster
        st.subheader("Rotte per Cluster")
        selected_cluster = st.selectbox(
            "Seleziona un cluster da visualizzare:",
            sorted(clusters_df["cluster"].unique())
        )
        
        cluster_routes = clusters_df[clusters_df["cluster"] == selected_cluster].sort_values(
            "voli_totali", ascending=False
        )
        st.dataframe(cluster_routes)
        
    except Exception as e:
        st.error(f"Errore nell'analisi: {str(e)}")