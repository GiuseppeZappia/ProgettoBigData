# Progetto Big Data

Repository dedicata al progetto di Big Data per la realizzazione di un'applicazione che permetta di effettuare interrogazioni aggregate su un dataset contenente informazioni di dettaglio su un gran numero di voli effettuati sul territorio americano usando Spark.

## Funzionalità principali
- Analisi aggregata dei dati di volo.
- Preprocessing dei dati per identificare anomalie e gestire valori mancanti.
- Calcolo di statistiche su tratte, aeroporti e compagnie aeree.
- Clustering di tratte e aeroporti basato su metriche avanzate.
- Predizioni e analisi di cancellazioni dei voli.

## Requisiti
Assicurati di avere installati i seguenti strumenti e librerie:
- Python 3.7 o superiore
- Apache Spark
- Streamlit
- Librerie Python necessarie 
  ```
  pyspark
  pandas
  geopy
  streamlit
  ```

## Installazione
1. Clona questa repository sul tuo computer locale:
   ```bash
   git clone https://github.com/tuo-username/ProgettoBigData.git
   ```

2. Naviga nella directory del progetto:
   ```bash
   cd ProgettoBigData
   ```

3. Installa le dipendenze richieste

## Avvio del progetto
Per avviare l'applicazione, esegui il seguente comando nel terminale dalla directory principale del progetto:
```bash
streamlit run Home.py
```

### Nota
Assicurati che:
- Tutti i file richiesti dal progetto (come i dataset) siano posizionati nelle directory corrette.
- Le configurazioni necessarie per Spark e Streamlit siano correttamente impostate.
- Se stai lavorando su una macchina locale, il file `Home.py` si trovi nella directory principale del progetto.

## Contributi
Sentiti libero di contribuire al progetto tramite:
- Pull Request
- Segnalazioni di bug
- Suggerimenti per nuove funzionalità

## Licenza
Questo progetto è rilasciato sotto la licenza MIT. Consulta il file `LICENSE` per maggiori dettagli.

