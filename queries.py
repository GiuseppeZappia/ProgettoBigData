import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, sum, asc, desc, when,avg,expr,coalesce, stddev,lit
from geopy import Nominatim
import pandas as pd
from pyspark.sql.functions import round as pyspark_round
import streamlit as st
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.clustering import KMeans
import time

# folder_path = r"C:\Users\xdomy\Desktop\Università\MAGISTRALE\1° Anno 1° Semestre\Modelli e Tecniche per Big Data\Progetto Voli\Dataset Voli"

spark=SparkSession.builder.master("local[*]").appName("Progetto BigData.com").config("spark.driver.memory", "8g").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

folder_path = r"C:\Users\giuse\Desktop\UNIVERSITA'\MAGISTRALE\1° ANNO\1° SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\DATI"
file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

#df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).cache()

#df.printSchema()


df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).drop("_c109").cache()


#---------------------------------------------QUERY PER VERIFICARE SE IL DATASET HA BISOGNO DI PREPROCESSING---------------------------------------------

coordinateAeroporti = "C:/Users/giuse/Desktop/UNIVERSITA'/MAGISTRALE/1° ANNO/1° SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/coordinate.csv"
# coordinateAeroporti = "C:/Users/xdomy/Desktop/Università/MAGISTRALE/1° Anno 1° Semestre/Modelli e Tecniche per Big Data/Progetto Voli/ProgettoBigData/coordinate.csv"
coordinate_aeroporto="C:/Users/giuse/Desktop/UNIVERSITA'/MAGISTRALE/1° ANNO/1° SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/output_airports.csv"
#coordinate_aeroporto domenico

def conta_righe_duplicate():
    df.groupBy(df.columns).count().filter("count > 1").count()  

# Colonne e condizioni per il controllo delle anomalie 
anomalies_conditions = {
    "Distance": "col('Distance') <= 0",  # Distanza non positiva
    "AirTime": "col('AirTime') <= 0",  # Tempo di volo non positivo
    "CRSElapsedTime": "col('CRSElapsedTime') <= 0",  # Tempo previsto di volo non positivo
    "ActualElapsedTime": "col('ActualElapsedTime') <= 0",  # Tempo reale di volo non positivo
    "Flights": "col('Flights') <= 0",  # Numero di voli non positivo
    "DepDelayMinutes": "col('DepDelayMinutes') < 0",  # Minuti di ritardo negativi
    "ArrDelayMinutes": "col('ArrDelayMinutes') < 0",  # Minuti di ritardo di arrivo negativi
    "DepDelay": "(col('DepDelay') > 0) & (col('DepDelay') != col('DepDelayMinutes'))",  #colonne che dovrebbero essere uguali non lo sono (tranne casi in cui una delle due è negativa e l'altra 0)
}

# Ciclo per eseguire i controlli su ciascuna colonna
def analisi_dataset():
    for column, condition in anomalies_conditions.items():
        print(f"Analizzando la colonnaa: {column}")
        anomalies = df.filter(eval(condition))  # eval valuta la condizione scritta come stringa
        print(f"Numero di anomalie in {column}: {anomalies.count()}")


#---------------------------------------------QUERY GENERALI---------------------------------------------

def coordinate(origin, originCityName, destination, destinationCityName):
    locator = Nominatim(user_agent="myNewGeocoder")
    found = locator.geocode(origin + " Airport " + originCityName, timeout=None)
    if found is None:
        time.sleep(1)
        found = locator.geocode(originCityName, timeout=None)
    result = [found.latitude, found.longitude]
    time.sleep(1)
    found = locator.geocode(destination + " Airport " + destinationCityName, timeout=None)
    if found is None:
        time.sleep(1)
        found = locator.geocode(destinationCityName, timeout=None)
    result.append(found.latitude)
    result.append(found.longitude)
    dfRes = pd.DataFrame({'OriginLatitude': [result[0]], 'OriginLongitude': [result[1]],
                              'DestLatitude': [result[2]], 'DestLongitude': [result[3]]})
    return dfRes

@st.cache_data(show_spinner=False)
def conta_righe_totali():
    return df.count()


def codici_aeroporti():
    aeroporti_origine = df.select("Origin").distinct()
    aeroporti_destinazione = df.select("Dest").distinct()
    tutti = aeroporti_origine.union(aeroporti_destinazione).distinct()
    return tutti


def codici_compagnie_aeree():
    return df.select("Reporting_Airline").distinct()


def trova_citta_da_aeroporto(codice_aeroporto):
    origine = df.filter((col("Origin") == codice_aeroporto)).select("OriginCityName").distinct().collect()
    if origine:
        return origine[0]["OriginCityName"]
    destinazione=df.filter((col("Dest") == codice_aeroporto)).select("DestinationCityName").distinct().collect()
    if destinazione:
        return destinazione[0]["DestinationCityName"]
    return "Aeroporto non trovato"

@st.cache_data(show_spinner=False)
def numero_voli_per_aeroporto(codice_aeroporto,data=None):
    if data is None:
        voli_filtrati = df.filter((col("Origin") == codice_aeroporto) | (col("Dest") == codice_aeroporto))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli
    if data:
        voli_filtrati = df.filter(((col("Origin") == codice_aeroporto) | (col("Dest") == codice_aeroporto)) & (col("FlightDate") == data))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli

@st.cache_data(show_spinner=False)
def numero_voli_per_compagnia(compagnia,data=None):
    if data is None:
        voli_filtrati = df.filter((col("Reporting_Airline") == compagnia))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli
    if data:
        voli_filtrati = df.filter((col("Reporting_Airline") == compagnia) & (col("FlightDate") == data))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli

def volo_distanza_max_compagnia(compagnia):
    volo=df.filter((col("Reporting_Airline")==compagnia)).orderBy(col("Distance").desc()).limit(1)
    return volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance")

def volo_distanza_max_verso_aeroporto(aeroporto):
    volo=df.filter((col("Dest")==aeroporto)).orderBy(col("Distance").desc()).limit(1)
    return volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance")

def volo_distanza_min_verso_aeroporto(aeroporto_selezionato):
    volo=df.filter((col("Dest")==aeroporto_selezionato)).orderBy(col("Distance").asc()).limit(1)
    return volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance")

def volo_distanza_min_da_aeroporto(aeroporto_selezionato):
    volo=df.filter((col("Origin")==aeroporto_selezionato)).orderBy(col("Distance").asc()).limit(1)
    return volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance")

def volo_distanza_min_compagnia(compagnia):
    volo=df.filter((col("Reporting_Airline")==compagnia)).orderBy(col("Distance").asc()).limit(1)
    return volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance")

def tratta_piu_percorsa_da_aeroporto(aeroporto_selezionato):
    tratta=df.filter((col("Origin")==aeroporto_selezionato)).groupBy("Origin","Dest","OriginCityName", "DestCityName").count().orderBy(desc("count")).limit(1)
    return tratta.select("Origin", "Dest", "OriginCityName", "DestCityName","count")

def tratta_piu_percorsa_compagnia(compagnia):
    tratta=df.filter((col("Reporting_Airline")==compagnia)).groupBy("Origin","Dest","OriginCityName", "DestCityName").count().orderBy(desc("count")).limit(1)
    return tratta.select("Origin", "Dest", "OriginCityName", "DestCityName","count")

def tratta_piu_percorsa_verso_aeroporto(aeroporto_selezionato):
    tratta=df.filter((col("Dest")==aeroporto_selezionato)).groupBy("Origin","Dest","OriginCityName", "DestCityName").count().orderBy(desc("count")).limit(1)
    return tratta.select("Origin", "Dest", "OriginCityName", "DestCityName","count")

def volo_distanza_max(data_inizio=None, data_fine=None):
    query = df
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    volo = query.orderBy(col("Distance").desc()).limit(1)
    return volo

def volo_distanza_min(data_inizio=None, data_fine=None):
    query = df
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    volo = query.orderBy(col("Distance").asc()).limit(1)
    return volo

def tratte_con_piu_ritardi_totali(data_inizio=None, data_fine=None):
    query = df.filter((col("ArrDelayMinutes").isNotNull()) & (col("DepDelayMinutes").isNotNull()))
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte = query.groupBy("Origin", "Dest").agg(
        sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
        sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")
    ).withColumn(
        "TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")
    ).select("Origin", "Dest", "TotaleMinutiRitardo").orderBy(desc("TotaleMinutiRitardo")).limit(10)
    return tratte


def tratte_con_meno_ritardi_totali(data_inizio=None, data_fine=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte = query.groupBy("Origin", "Dest").agg(
        sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
        sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")
    ).withColumn(
        "TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")
    ).select("Origin", "Dest", "TotaleMinutiRitardo").orderBy(asc("TotaleMinutiRitardo")).limit(10)
    return tratte


def aereo_piu_km_percorsi(data_inizio=None, data_fine=None):
    query = df.filter(col("Tail_Number").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aereo = query.groupBy("Tail_Number").agg(
        sum("Distance").alias("TotalDistance")
    ).orderBy(col("TotalDistance").desc()).limit(1)
    return aereo

def velocita_media_totale(data_inizio=None, data_fine=None):
    query = df.filter((col("Distance").isNotNull()) & (col("AirTime").isNotNull()))
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    velocita = query.withColumn(
        "AverageSpeed", col("Distance") / (col("AirTime") / 60)
    ).select(avg("AverageSpeed").alias("AverageAircraftSpeed"))
    return velocita

def stati_piu_visitati():
  stati_filtrati= df.filter(col("DestStateName").isNotNull())
  stati=stati_filtrati.groupBy("DestStateName").count().orderBy(col("count").desc()).limit(10)
  return stati

def compagnie_piu_voli_fatti():
  compagnie_filtrate= df.filter(col("Reporting_Airline").isNotNull())
  compagnie=compagnie_filtrate.groupBy("Reporting_Airline").count().orderBy(col("count").desc()).limit(10)
  return compagnie


def get_aerei_disponibili(data_inizio=None, data_fine=None):
    if data_inizio and data_fine:
        query = df.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)).select("Tail_Number").distinct()
    query = df.select("Tail_Number").distinct().filter(col("Tail_Number").isNotNull())
    return query


def cancellazioniPerGiorno(data_inizio=None, data_fine=None, giorno=None):
    query = df
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    if giorno:
        cancellazioni = query.filter(col("DayOfWeek") == giorno) \
            .select(sum("Cancelled").alias("TotaleCancellazioni"))
        risultato = cancellazioni.collect()
        if risultato:
            totale = risultato[0]["TotaleCancellazioni"]
            return totale
        return None
    else:
        return query.groupBy("DayOfWeek") \
            .agg(sum("Cancelled").alias("Cancellazioni")) \
            .orderBy("DayOfWeek")


def cancellazioniPerCausa(data_inizio=None, data_fine=None, causa=None):
    query = df.filter(col("Cancelled") == 1)
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    if causa:
        cancellazioni = query.filter(col("CancellationCode") == causa) \
            .select(count("*").alias("TotaleCancellazioni"))
        risultato = cancellazioni.collect()
        if risultato:
            totale = risultato[0]["TotaleCancellazioni"]
            return totale
        return None
    else:
        return query.groupBy("CancellationCode") \
            .count() \
            .orderBy("count", ascending=False)


def velocita_media_per_tratta_specifica(origin, dest):
    velocita_filtrate = df.filter((col("Origin") == origin) & (col("Dest") == dest) & 
                                  col("Distance").isNotNull() & col("AirTime").isNotNull())
    velocita = velocita_filtrate.withColumn("AverageSpeed", col("Distance") / (col("AirTime") / 60)) \
                                .agg(avg("AverageSpeed").alias("AverageSpeedForRoute"))
    return velocita


def volo_distanza_max_da_aeroporto(aeroporto):
    volo=df.filter((col("Origin")==aeroporto)).orderBy(col("Distance").desc()).limit(1)
    return volo.select(col("Origin"),col("OriginCityName"),col("Dest"),col("DestCityName"),col("Distance"))

def tratte_distinte_per_aereo(tail_number, data_inizio=None, data_fine=None):
    query = df.filter((col("Tail_Number") == tail_number) & col("Origin").isNotNull() & col("Dest").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte_distinte = query.select("Origin", "OriginCityName", "Dest", "DestCityName").distinct()
    return tratte_distinte

def tratte_distinte_per_aeroporto(airport_code):
    tratte_filtrate = df.filter(((col("Origin") == airport_code) | (col("Dest") == airport_code)))
    tratte_distinte = tratte_filtrate.select(col("Origin"),col("OriginCityName"),col("Dest"),col("DestCityName")).distinct()
    return tratte_distinte 

def tratte_distinte_per_compagnia(compagnia):
    tratte_filtrate = df.filter(col("Reporting_Airline") == compagnia)
    tratte_distinte = tratte_filtrate.select(col("Origin"),col("OriginCityName"),col("Dest"),col("DestCityName")).distinct()
    return tratte_distinte 

def giorno_della_settimana_con_piu_voli(aeroporto=None,compagnia=None):
    if aeroporto:
        return df.filter((col("Dest")==aeroporto)| (col("Origin")==aeroporto)).groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1)
    if compagnia:
       return df.filter((col("Reporting_Airline")==compagnia)).groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1)
    return df.groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1)
    
def giorno_della_settimana_con_piu_voli_date(data_inizio=None, data_fine=None):
    query = df.filter(col("DayOfWeek").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    giorno = query.groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1)
    return giorno

def calcolo_ritardo_per_mesi(aeroporto,mese_inizio,mese_fine):
    ritardo_per_mesi = (df.filter((col("Origin") == aeroporto) & (col("Month") >= mese_inizio) & (col("Month") <= mese_fine))
                        .groupBy("Month").agg(pyspark_round(avg("DepDelay"),2).alias("ritardo_medio"),count("*").alias("voli_totali")).orderBy("Month"))
    return ritardo_per_mesi

def calcolo_ritardo_per_mesi_compagnia(compagnia,mese_inizio,mese_fine):
    ritardo_per_mesi = (df.filter((col("Reporting_Airline") == compagnia) & (col("Month") >= mese_inizio) & (col("Month") <= mese_fine))
                        .groupBy("Month").agg(pyspark_round(avg("DepDelay"),2).alias("ritardo_medio"),count("*").alias("voli_totali")).orderBy("Month"))
    return ritardo_per_mesi

def calcola_ritardo_giornaliero(aeroporto,data_inizio,data_fine):
    ritardo_giornaliero=df.filter((col("Origin") == aeroporto) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)).groupBy("FlightDate").agg(avg("DepDelay").alias("ritardo_medio_giornaliero"),count("*").alias("voli_totali")).orderBy("FlightDate") 
    return ritardo_giornaliero

def calcola_ritardo_giornaliero_compagnia(compagnia,data_inizio,data_fine):
    ritardo_giornaliero=df.filter((col("Reporting_Airline") == compagnia) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)).groupBy("FlightDate").agg(avg("DepDelay").alias("ritardo_medio_giornaliero"),count("*").alias("voli_totali")).orderBy("FlightDate")  
    return ritardo_giornaliero

@st.cache_data(show_spinner=False)
def totale_voli_cancellati(aeroporto=None,compagnia=None):
    cancellati=df.filter(col("Cancelled")=="1.00").count()
    if(aeroporto):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("Origin")==aeroporto)).count()
    if(compagnia):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("Reporting_Airline")==compagnia)).count()
    return cancellati

@st.cache_data(show_spinner=False)
def totale_voli_cancellati_date(data_inizio=None, data_fine=None):
    query = df.filter(col("Cancelled") == "1.00")
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    cancellati = query.count()
    return cancellati

def mese_con_piu_cancellati(data_inizio=None, data_fine=None):
    query = df.filter(col("Month").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    mesi = query.groupBy("Month").count().orderBy(col("count").desc()).limit(1)
    return mesi

def mese_con_meno_cancellati(data_inizio=None, data_fine=None):
    query = df.filter(col("Month").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    mesi = query.groupBy("Month").count().orderBy(col("count").asc()).limit(1)
    return mesi

@st.cache_data(show_spinner=False)
def percentuale_voli_orario(codice_aeroporto=None,compagnia=None):
    voli_filtrati=df.filter(col("ArrDelayMinutes").isNotNull())
    if compagnia is None and codice_aeroporto is None:
        return round((voli_filtrati.filter(col("ArrDelayMinutes")==0).count()*100)/voli_filtrati.count(),2)
    if codice_aeroporto:
        voli_filtrati= voli_filtrati.filter((col("Dest")==codice_aeroporto))
    if compagnia:
        voli_filtrati=voli_filtrati.filter((col("Reporting_Airline")==compagnia))
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")==0).count()*100)/voli_filtrati.count()
    return round(voli,2) #prime due cifre dopo virgola


@st.cache_data(show_spinner=False)
def percentuale_voli_ritardo_date(data_inizio=None, data_fine=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    voli_totali = query.count()
    in_ritardo = query.filter(col("ArrDelayMinutes") > 0).count()
    percentuale = (in_ritardo / voli_totali) * 100 if voli_totali > 0 else 0
    return round(percentuale, 2)

@st.cache_data(show_spinner=False)
def percentuale_voli_ritardo(aeroporto=None,compagnia=None):
    voli_filtrati=df.filter(col("ArrDelayMinutes").isNotNull())
    if compagnia is None and aeroporto is None:
        return round((voli_filtrati.filter(col("ArrDelayMinutes")>0).count()*100)/voli_filtrati.count(),2)
    if aeroporto:
        voli_filtrati= voli_filtrati.filter((col("Dest")==aeroporto))
    if compagnia:  
        voli_filtrati=voli_filtrati.filter((col("Reporting_Airline")==compagnia))
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")>0).count()*100)/voli_filtrati.count()
    return round(voli,2) 


@st.cache_data(show_spinner=False)
def totale_voli_da_aeroporto(aeroporto,data=None):
    if(data is None):
        return df.filter((col("Origin")==aeroporto)).count()
    if(data):
        return df.filter((col("Origin")==aeroporto)&(col("FlightDate")==data)).count()
    
@st.cache_data(show_spinner=False)
def totale_voli_verso_aeroporto(aeroporto,data=None):
    if(data is None):
        return df.filter((col("Dest")==aeroporto)).count()
    if(data):
        return df.filter((col("Dest")==aeroporto)&(col("FlightDate")==data)).count()

@st.cache_data(show_spinner=False)
def percentuale_voli_cancellati_aeroporto(aeroporto,data=None):
    if(data is None):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("Origin")==aeroporto)).count()
        voli_totali_da_aeroorto=totale_voli_da_aeroporto(aeroporto)
        if(voli_totali_da_aeroorto==0):
            return 0
        perc=cancellati*100/voli_totali_da_aeroorto
        return round(perc,2)
    if(data):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("Origin")==aeroporto)&(col("FlightDate")==data)).count()
        voli_totali_da_aeroorto=totale_voli_da_aeroporto(aeroporto,data)
        if(voli_totali_da_aeroorto==0):
            return 0
        perc=cancellati*100/voli_totali_da_aeroorto
        return round(perc,2)

@st.cache_data(show_spinner=False)
def numero_voli_per_compagnia(compagnia,data=None):
    if data is None:
        voli_filtrati = df.filter((col("Reporting_Airline") ==compagnia))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli
    if data:
        voli_filtrati = df.filter((col("Reporting_Airline") == compagnia) & (col("FlightDate") == data))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli


@st.cache_data(show_spinner=False)
def totale_voli_compagnia(compagnia,data=None):
    if data:
        voli_filtrati = df.filter((col("Reporting_Airline") == compagnia) & (col("FlightDate") == data))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli
    compagnie_filtrate=df.filter((col("Reporting_Airline")==compagnia)).count()
    return compagnie_filtrate

@st.cache_data(show_spinner=False)
def percentuale_voli_cancellati_compagnia(compagnia,data=None):
    if data is None:
        voli_filtrati = df.filter((col("Cancelled") == 1.00) & (col("Reporting_Airline") == compagnia))
        cancellati = voli_filtrati.count()
        voli_totali_compagnia=totale_voli_compagnia(compagnia)
        if(voli_totali_compagnia==0):
            return 0
        perc=cancellati*100/voli_totali_compagnia
        return round(perc,2)
    if data:
        voli_filtrati = df.filter((col("Cancelled") == 1.00) & (col("Reporting_Airline") == compagnia) & (col("FlightDate") == data))
        cancellati = voli_filtrati.count()
        voli_totali_compagnia=totale_voli_compagnia(compagnia,data)
        if(voli_totali_compagnia==0):
            return 0
        perc=cancellati*100/voli_totali_compagnia
        return round(perc,2)


def percentuali_cause_ritardo(filtro_compagnia=None, causa_specifica=None, data_inizio=None, data_fine=None,stato=None,aeroporto=None):
    df_filtrato = df
    if aeroporto:
        df_filtrato = df_filtrato.filter((col("Origin")==aeroporto))
    if stato:
        df_filtrato=df_filtrato.filter((col("OriginStateName")==stato) | (col("DestStateName")==stato))
    if filtro_compagnia:
        df_filtrato = df_filtrato.filter(col("Reporting_Airline") == filtro_compagnia)
    if data_inizio and data_fine:
        df_filtrato = df_filtrato.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
   
    df_filtrato = df_filtrato.fillna(0, subset=["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"])

    ritardi_cause = df_filtrato.select(
        ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
    ).agg(
        sum("CarrierDelay").alias("CarrierDelay"),
        sum("WeatherDelay").alias("WeatherDelay"),
        sum("NASDelay").alias("NASDelay"),
        sum("SecurityDelay").alias("SecurityDelay"),
        sum("LateAircraftDelay").alias("LateAircraftDelay")
    )

    ritardi_somma = ritardi_cause.select(
        (col("CarrierDelay") +
         col("WeatherDelay") +
         col("NASDelay") +
         col("SecurityDelay") +
         col("LateAircraftDelay")).alias("TotaleRitardo")
    ).collect()[0]["TotaleRitardo"]  

    if causa_specifica:
        ritardi_percentuali = ritardi_cause.select(
            pyspark_round((col(causa_specifica) / ritardi_somma * 100),2).alias(f"{causa_specifica}_Percent")
        )
    else:
        ritardi_percentuali = ritardi_cause.select(
            pyspark_round((col("CarrierDelay") / ritardi_somma * 100),2).alias("Carrier"),
            pyspark_round((col("WeatherDelay") / ritardi_somma * 100),2).alias("Weather"),
            pyspark_round((col("NASDelay") / ritardi_somma * 100),2).alias("NAS"),
            pyspark_round((col("SecurityDelay") / ritardi_somma * 100),2).alias("Security"),
            pyspark_round((col("LateAircraftDelay") / ritardi_somma * 100),2).alias("Late Aircraft")
        )
    return ritardi_percentuali

def ritardo_medio_per_stagione(aeroporto=None,compagnia=None):

    season_mapping = {
        "Inverno": [12, 1, 2],
        "Primavera": [3, 4, 5],
        "Estate": [6, 7, 8],
        "Autunno": [9, 10, 11],
    }
    df_filtrato = df
    
    if aeroporto:
        df_filtrato = df_filtrato.filter(col("Origin") == aeroporto)
    if compagnia:
        df_filtrato = df_filtrato.filter(col("Reporting_Airline") == compagnia)

    df1 = df_filtrato.withColumn(
        "Stagione",
        when(col("Month").isin(season_mapping["Inverno"]), "Inverno")
         .when(col("Month").isin(season_mapping["Primavera"]), "Primavera")
         .when(col("Month").isin(season_mapping["Estate"]), "Estate")
         .when(col("Month").isin(season_mapping["Autunno"]), "Autunno")
    )

    avg_delays = df1.groupBy("Stagione").agg(avg("ArrDelayMinutes").alias("AvgDelay"))
    
    result = avg_delays.orderBy(expr(
        "case Stagione when 'Inverno' then 1 when 'Primavera' then 2 when 'Estate' then 3 when 'Autunno' then 4 end"
    ))
    return result


def stati_con_maggiore_increm_ritardo_inverno_rispetto_estate(data_inizio=None,data_fine=None):
    mesi_invernali = [12, 1, 2]
    mesi_estivi = [6, 7, 8]
    if(data_inizio and data_fine):
        ritardi_invernali = df.filter((col("Month").isin(mesi_invernali)) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)) \
                        .groupBy("OriginStateName") \
                        .agg(avg("ArrDelayMinutes").alias("WinterAvgDelay"))
        ritardi_estivi = df.filter((col("Month").isin(mesi_estivi)) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)) \
                        .groupBy("OriginStateName") \
                        .agg(avg("ArrDelayMinutes").alias("SummerAvgDelay"))
       
        confronto_ritardi = ritardi_invernali.join(ritardi_estivi, on="OriginStateName", how="inner")
        
        confronto_ritardi = confronto_ritardi.withColumn(
            "IncrementoRitardi", col("WinterAvgDelay") - col("SummerAvgDelay")
        )
        
        result = confronto_ritardi.orderBy(col("IncrementoRitardi").desc()).limit(10)
        return result
    else:
        ritardi_invernali = df.filter(col("Month").isin(mesi_invernali)) \
                        .groupBy("OriginStateName") \
                        .agg(avg("ArrDelayMinutes").alias("WinterAvgDelay"))
        
        ritardi_estivi = df.filter(col("Month").isin(mesi_estivi)) \
                        .groupBy("OriginStateName") \
                        .agg(avg("ArrDelayMinutes").alias("SummerAvgDelay"))
        

        confronto_ritardi = ritardi_invernali.join(ritardi_estivi, on="OriginStateName", how="inner")

        confronto_ritardi = confronto_ritardi.withColumn(
            "IncrementoRitardi", col("WinterAvgDelay") - col("SummerAvgDelay")
        )

        result = confronto_ritardi.orderBy(col("IncrementoRitardi").desc()).limit(10)
        return result


def rottePiuComuni(data_inizio=None, data_fine=None):
    query = df.filter(col("Origin").isNotNull() & col("Dest").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    rotte = query.groupBy("Origin", "Dest").agg(count("*").alias("NumeroVoli"))
    rotte_piu_comuni = rotte.orderBy(col("NumeroVoli").desc()).limit(10)
    return rotte_piu_comuni



def compagniaPiuKmPercorsi():
    voli_con_tail_number = df.filter(col("Tail_Number").isNotNull())
    km_percorsi_per_compagnia = voli_con_tail_number.groupBy("Reporting_Airline").agg(sum("Distance").alias("TotaleKm"))
    compagnia_max_km = km_percorsi_per_compagnia.orderBy(col("TotaleKm").desc()).limit(10)
    return compagnia_max_km

def statiMinRitardoMedio():
    tratte_filtrate = df.filter(col("ArrDelayMinutes").isNotNull())
    ritardo_medio_per_stato = tratte_filtrate.groupBy("DestStateName").agg(mean("ArrDelayMinutes").alias("RitardoMedio"))
    stati_min_ritardo = ritardo_medio_per_stato.orderBy(col("RitardoMedio")).limit(10)
    stati_min_ritardo = stati_min_ritardo.withColumn('RitardoMedio', pyspark_round(col('RitardoMedio'), 2))
    return stati_min_ritardo

def totaleVoliPerMese(aeroporto=None,compagnia=None):
    voli_per_mese = df.groupBy("Month").agg(count("*").alias("NumeroVoli")).orderBy("Month")
    if(aeroporto):
        voli_per_mese = df.filter((col("Origin") == aeroporto)|(col("Dest") == aeroporto)).groupBy("Month").agg(count("*").alias("NumeroVoli")).orderBy("Month")
    if(compagnia):
        voli_per_mese = df.filter((col("Reporting_Airline") == compagnia)).groupBy("Month").agg(count("*").alias("NumeroVoli")).orderBy("Month")
    return voli_per_mese

def totaleVoliPerMeseDate(data_inizio=None, data_fine=None):
    query = df.filter(col("Month").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    voli_per_mese = query.groupBy("Month").agg(count("*").alias("NumeroVoli")).orderBy("Month")
    return voli_per_mese

def totale_voli_cancellati_per_mese(aeroporto=None,compagnia=None):
    voli_cancellati_per_mese = df.filter(col("Cancelled") == 1.00).groupBy("Month").agg(count("*").alias("NumeroVoliCancellati")).orderBy("Month")
    if(aeroporto):
        voli_cancellati_per_mese = df.filter((col("Origin") == aeroporto)).filter(col("Cancelled") == 1.00).groupBy("Month").agg(count("*").alias("NumeroVoliCancellati")).orderBy("Month")
    if(compagnia):
        voli_cancellati_per_mese = df.filter((col("Reporting_Airline") == compagnia)).filter(col("Cancelled") == 1.00).groupBy("Month").agg(count("*").alias("NumeroVoliCancellati")).orderBy("Month")
    return voli_cancellati_per_mese

def percentualeVoliInOrario(data_inizio=None, data_fine=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    voli_totali = query.count()
    in_orario = query.filter(col("ArrDelayMinutes") <= 0).count()
    percentuale = (in_orario / voli_totali) * 100 if voli_totali > 0 else 0
    return round(percentuale, 2)


@st.cache_data(show_spinner=False)
def voliRitardoDecolloArrivoAnticipo(data_inizio=None, data_fine=None):
    query = df.filter((col("DepDelay") > 0) & (col("ArrDelay") < 0))
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    numero_voli = query.count()
    return numero_voli
    

def stato_piu_visitato_date(data_inizio=None, data_fine=None):
    query = df.filter(col("DestStateName").isNotNull())
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    stati_visitati = query.groupBy("DestStateName").agg(count("*").alias("TotaleVisite"))
    
    stato_piu_visitato = stati_visitati.orderBy(col("TotaleVisite").desc()).limit(1)
    
    return {"stato_piu_visitato": stato_piu_visitato}


def ritardo_medio_per_stato(data_inizio=None, data_fine=None):
    query = df.filter(col("OriginStateName").isNotNull() & col("DepDelay").isNotNull())
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    ritardi = query.groupBy("OriginStateName").agg(avg("DepDelay").alias("RitardoMedio"))
    
    ritardi_ordinati = ritardi.orderBy(col("RitardoMedio").desc())
    
    return ritardi_ordinati


def voli_per_stato_origine(data_inizio=None, data_fine=None):
    query = df.filter(col("OriginStateName").isNotNull())
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    voli = query.groupBy("OriginStateName").agg(count("*").alias("TotaleVoli"))
    
    voli_ordinati = voli.orderBy(col("TotaleVoli").desc())
    
    return voli_ordinati


def rapporto_voli_cancellati(data_inizio=None, data_fine=None, stato=None):
    query = df.filter(col("OriginStateName").isNotNull())

    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))

    if stato:
        query = query.filter(col("OriginStateName") == stato)

    rapporto = query.groupBy("OriginStateName").agg(
        count("*").alias("TotaleVoli"),
        sum(col("Cancelled").cast("int")).alias("VoliCancellati")
    ).withColumn(
        "RapportoCancellati", col("VoliCancellati") / col("TotaleVoli")
    )

    rapporto_ordinato = rapporto.orderBy(col("RapportoCancellati").desc())
    return rapporto_ordinato


def tempi_volo_per_stato(data_inizio=None, data_fine=None, stato=None):
    query = df.filter(col("OriginStateName").isNotNull() & col("AirTime").isNotNull())

    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    if stato:
        query = query.filter(col("OriginStateName") == stato)
    
    volo_piu_veloce = query.orderBy(col("AirTime").asc()).limit(1)

    volo_piu_lento = query.orderBy(col("AirTime").desc()).limit(1)
    
    return {
        "volo_piu_veloce": volo_piu_veloce,
        "volo_piu_lento": volo_piu_lento
    }


def ritardi_medi_per_giorno(data_inizio=None, data_fine=None, giorno=None):
    query = df.filter(col("OriginStateName").isNotNull() & col("DepDelay").isNotNull())

    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    if giorno is not None:
        query = query.filter(col("DayOfWeek") == giorno)

    ritardi = query.groupBy("OriginStateName").agg(avg("DepDelay").alias("RitardoMedio"))
    return ritardi.orderBy(col("RitardoMedio").desc())


def incremento_ritardi_invernali(causa=None):
    query = df.filter(
        col("OriginStateName").isNotNull() &
        col("DepDelay").isNotNull() &
        col("Month").isin([12, 1, 2])  
    )

    if causa:
        query = query.filter(col("CancellationCode") == causa)

    ritardi = query.groupBy("OriginStateName").agg(
        avg("DepDelay").alias("RitardoMedio")
    )

    return ritardi.orderBy(col("RitardoMedio").desc())


def stati_con_minor_ritardo_date(data_inizio=None, data_fine=None):
    query = df.filter(col("OriginStateName").isNotNull() & col("DepDelay").isNotNull())
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    ritardi = query.groupBy("OriginStateName").agg(avg("DepDelay").alias("RitardoMedio"))
    
    return ritardi.orderBy(col("RitardoMedio").asc())


def stati_piu_visitati_date(data_inizio=None, data_fine=None):
    query = df.filter(col("DestStateName").isNotNull())
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    voli = query.groupBy("DestStateName").agg(count("*").alias("TotaleVoli"))
    
    return voli.orderBy(col("TotaleVoli").desc())

def stati_maggiore_traffico_date(data_inizio=None, data_fine=None):
    query = df.filter(
        (col("OriginStateName").isNotNull()) & 
        (col("DestStateName").isNotNull()) &
        (col("Distance").isNotNull())  
    )
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    traffico = query.groupBy("OriginStateName").agg(
        sum("Distance").alias("ChilometriTotali")  
    )
    
    return traffico.orderBy(col("ChilometriTotali").desc())


def percentuali_cause_cancellazioni(data_inizio=None, data_fine=None):
    query = df.filter(col("Cancelled") == 1)  

    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    cause = query.groupBy("CancellationCode").agg(
        count("*").alias("TotaleCasi")
    ).withColumn(
        "Percentuale", (col("TotaleCasi") / query.count()) * 100
    )

    return cause.orderBy(col("Percentuale").desc())


def stati_efficienza(data_inizio=None, data_fine=None):
    query = df.filter(
        (col("OriginStateName").isNotNull()) & (col("ArrDelay").isNotNull()) & (col("DepDelay").isNotNull())
    )
    
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    efficienza = query.groupBy("OriginStateName").agg(
        avg(col("ArrDelay") + col("DepDelay")).alias("RitardoMedio"),
        count("*").alias("NumeroVoli")
    ).withColumn(
        "Efficienza", col("RitardoMedio") / col("NumeroVoli")  
    ).select("OriginStateName", "Efficienza").orderBy("Efficienza")  
    
    return efficienza


def stagionalita_voli_per_stato(data_inizio=None, data_fine=None, stato_selezionato=None):
    query = df.filter(
        (col("OriginStateName").isNotNull()) & (col("Month").isNotNull())
    )

    if stato_selezionato:
        query = query.filter(col("OriginStateName") == stato_selezionato)

    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))

    stagionalita = query.groupBy("Month").agg(
        count("*").alias("NumeroVoli")
    ).orderBy("Month")

    return stagionalita

def stati_distinti():
    stati = df.select("OriginStateName").distinct()
    return stati


@st.cache_data(show_spinner=False)
def totale_voli_da_stato(stato,data=None):
    if(data is None):
        return df.filter((col("OriginStateName")==stato)).count()
    if(data):
        return df.filter((col("OriginStateName")==stato)&(col("FlightDate")==data)).count()
    
@st.cache_data(show_spinner=False)
def totale_voli_verso_stato(stato,data=None):
    if(data is None):
        return df.filter((col("DestStateName")==stato)).count()
    if(data):
        return df.filter((col("DestStateName")==stato)&(col("FlightDate")==data)).count()

@st.cache_data(show_spinner=False)
def numero_voli_per_stato(stato,data=None):
    if data is None:
        voli_filtrati = df.filter((col("OriginStateName") == stato) | (col("DestStateName") == stato))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli
    if data:
        voli_filtrati = df.filter(((col("OriginStateName") == stato) | (col("DestStateName") == stato)) & (col("FlightDate") == data))
        numero_di_voli = voli_filtrati.count()
        return numero_di_voli

@st.cache_data(show_spinner=False)
def percentuale_voli_cancellati_stato(stato,data=None):
    if(data is None):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("OriginStateName")==stato)).count()
        voli_totali_da_stato=totale_voli_da_stato(stato)
        if(voli_totali_da_stato==0):
            return 0
        perc=cancellati*100/voli_totali_da_stato
        return round(perc,2)
    if(data):
        cancellati=df.filter((col("Cancelled")==1.00)&(col("OriginStateName")==stato)&(col("FlightDate")==data)).count()
        voli_totali_da_stato=totale_voli_da_stato(stato,data)
        if(voli_totali_da_stato==0):
            return 0
        perc=cancellati*100/voli_totali_da_stato
        return round(perc,2)


#-------------------------KMEANS-------------------------

def prepare_airport_features():    
    #metriche per gli aeroporti di partenza
    dep_metrics = df.groupBy("Origin").agg(
        count("*").alias("voli_partenza"),
        mean("DepDelay").alias("media_ritardo_partenza"),
        mean("Distance").alias("media_distanza_partenza"),
        mean(col("Cancelled")).alias("rate_cancellazione_partenza")
    ).alias("dep")  

    #metriche per gli aeroporti di partenza
    arr_metrics = df.groupBy("Dest").agg(
        count("*").alias("voli_arrivo"),
        mean("ArrDelay").alias("media_ritardo_arrivo"),
        mean("Distance").alias("media_distanza_arrivo"),
        mean(col("Cancelled")).alias("rate_cancellazione_arrivo")
    ).alias("arr")  
    
    # Combino le metriche per ogni aeroporto usando riferimenti specifici
    features_df = dep_metrics.join(
        arr_metrics,
        col("dep.Origin") == col("arr.Dest"),
        "outer"
    ).select(
        coalesce(col("dep.Origin"), col("arr.Dest")).alias("airport_code"),
        (coalesce(col("dep.voli_partenza"), lit(0)) + 
         coalesce(col("arr.voli_arrivo"), lit(0))).alias("voli_totali"),
        ((coalesce(col("dep.media_ritardo_partenza"), lit(0)) * coalesce(col("dep.voli_partenza"), lit(0)) +
          coalesce(col("arr.media_ritardo_arrivo"), lit(0)) * coalesce(col("arr.voli_arrivo"), lit(0))) /
         (coalesce(col("dep.voli_partenza"), lit(0)) + 
          coalesce(col("arr.voli_arrivo"), lit(0)))).alias("ritardo_medio"),
        ((coalesce(col("dep.media_distanza_partenza"), lit(0)) * coalesce(col("dep.voli_partenza"), lit(0)) +
          coalesce(col("arr.media_distanza_arrivo"), lit(0)) * coalesce(col("arr.voli_arrivo"), lit(0))) /
         (coalesce(col("dep.voli_partenza"), lit(0)) + 
          coalesce(col("arr.voli_arrivo"), lit(0)))).alias("distanza_media"),
        ((coalesce(col("dep.rate_cancellazione_partenza"), lit(0)) * coalesce(col("dep.voli_partenza"), lit(0)) +
          coalesce(col("arr.rate_cancellazione_arrivo"), lit(0)) * coalesce(col("arr.voli_arrivo"), lit(0))) /
         (coalesce(col("dep.voli_partenza"), lit(0)) + 
          coalesce(col("arr.voli_arrivo"), lit(0)))).alias("rate_cancellazione")
    )
    
    return features_df


@st.cache_data(show_spinner=False)
def cluster_airports(numero_di_cluster):    
    #Preparo le features
    features_df = prepare_airport_features()
    #Assemblo il vettore delle features
    assembler = VectorAssembler(
        inputCols=["voli_totali", "ritardo_medio", "distanza_media", "rate_cancellazione"],
        outputCol="features"
    )
    #Standardizzo le features
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    #Applico K-means
    kmeans = KMeans(
        k=numero_di_cluster,
        featuresCol="scaled_features",
        predictionCol="cluster"
    )
    #Pipeline di trasformazione
    vector_df = assembler.transform(features_df)
    scaled_df = scaler.fit(vector_df).transform(vector_df)
    clusters = kmeans.fit(scaled_df).transform(scaled_df)
    #Converto in pandas per la visualizzazione
    return clusters.select("airport_code", "voli_totali", "ritardo_medio", "distanza_media", "rate_cancellazione", "cluster").toPandas()


#-------------------------TRATTE-------------------------
def prepare_route_features():    
    routes = df.groupBy("Origin", "Dest", "OriginCityName", "DestCityName").agg(
        count("*").alias("voli_totali"),
        coalesce(mean("Distance"), lit(0)).alias("distanza"),
        coalesce(mean("DepDelay"), lit(0)).alias("media_ritardo_partenza"),
        coalesce(mean("ArrDelay"), lit(0)).alias("media_ritardo_arrivo"),
        coalesce(mean("ActualElapsedTime"), lit(0)).alias("media_tempo_volo"),
        coalesce(mean(col("Cancelled")), lit(0)).alias("rate_cancellazione"),
        coalesce(mean(col("Diverted")), lit(0)).alias("rate_dirottamento"),
        coalesce(stddev("DepDelay"), lit(0)).alias("varianza_ritardo")
    )
    
    #Calcolo metriche aggregate con gestione dei null
    routes = routes.withColumn(
        "ritardo_medio", 
        coalesce(
            (col("media_ritardo_partenza") + col("media_ritardo_arrivo")) / 2,
            lit(0)
        )
    )
    
    #Mi assicuro che non ci siano valori null nelle colonne usate per il clustering
    routes = routes.select(
        "Origin",
        "Dest",
        "OriginCityName",
        "DestCityName",
        coalesce(col("voli_totali"), lit(0)).alias("voli_totali"),
        coalesce(col("distanza"), lit(0)).alias("distanza"),
        coalesce(col("ritardo_medio"), lit(0)).alias("ritardo_medio"),
        coalesce(col("varianza_ritardo"), lit(0)).alias("varianza_ritardo"),
        coalesce(col("rate_cancellazione"), lit(0)).alias("rate_cancellazione"),
        coalesce(col("rate_dirottamento"), lit(0)).alias("rate_dirottamento"),
        coalesce(col("media_tempo_volo"), lit(0)).alias("media_tempo_volo")
    )
    
    return routes

def cluster_routes(numero_cluster):    
    #Preparo le features
    features_df = prepare_route_features()
    #Assemblo il vettore delle features
    assembler = VectorAssembler(
        inputCols=[
            "voli_totali", 
            "distanza", 
            "ritardo_medio",
            "varianza_ritardo",
            "rate_cancellazione",
            "rate_dirottamento",
            "media_tempo_volo"
        ],
        outputCol="features"
    )
    #Standardizzo le features
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    #Applico K-means
    kmeans = KMeans(
        k=numero_cluster,  
        featuresCol="scaled_features",
        predictionCol="cluster"
    )
    
    #Pipeline di trasformazione
    vector_df = assembler.transform(features_df)
    scaled_df = scaler.fit(vector_df).transform(vector_df)
    clusters = kmeans.fit(scaled_df).transform(scaled_df)
    
    return clusters.select(
        "Origin", "Dest", "OriginCityName", "DestCityName",
        "voli_totali", "distanza", "ritardo_medio", "varianza_ritardo",
        "rate_cancellazione", "rate_dirottamento", "media_tempo_volo",
        "cluster"
    ).toPandas()


#--------------------PREDICITON CANCELLED------------------
# import streamlit as st
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, hour, when, coalesce, lit
# from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType
# from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
# from pyspark.ml.classification import LogisticRegression
# from pyspark.ml import Pipeline
# import datetime

# def prepare_training_data(df):
#     """
#     Prepare the training data with consistent feature engineering.
#     This function handles the initial data processing for model training.
#     """
#     # First, select our core features
#     selected_features = df.select(
#         "Origin", 
#         "Dest",
#         "Month",
#         "DayofMonth",
#         "CRSDepTime",
#         col("Cancelled").cast("double")
#     )
    
#     # Process CRSDepTime to extract hour - this is a critical step
#     # that needs to be consistent between training and prediction
#     processed_df = selected_features.withColumn(
#         "CRSDepTime_hour",
#         when(col("CRSDepTime").isNotNull(),
#              hour(coalesce(col("CRSDepTime").cast("string").cast("timestamp"), 
#                           lit("00:00:00").cast("timestamp")))
#         ).otherwise(0)
#     )
    
#     # Fill null values with sensible defaults
#     processed_df = processed_df.na.fill({
#         "Month": 1,
#         "DayofMonth": 1,
#         "CRSDepTime_hour": 0,
#         "Cancelled": 0.0
#     })
    
#     return processed_df

# def create_pipeline():
#     """
#     Create the ML pipeline with consistent feature processing steps.
#     Each stage of the pipeline is documented for clarity.
#     """
#     # Handle categorical variables for origin airport
#     origin_indexer = StringIndexer(
#         inputCol="Origin", 
#         outputCol="Origin_indexed",
#         handleInvalid="keep"  # Handle new categories gracefully
#     )
    
#     # Handle categorical variables for destination airport
#     dest_indexer = StringIndexer(
#         inputCol="Dest", 
#         outputCol="Dest_indexed",
#         handleInvalid="keep"
#     )
    
#     # Combine all features into a single vector
#     # Note that we use CRSDepTime_hour instead of DepHour
#     assembler = VectorAssembler(
#         inputCols=[
#             "Origin_indexed",
#             "Dest_indexed",
#             "Month",
#             "DayofMonth",
#             "CRSDepTime_hour"
#         ],
#         outputCol="features",
#         handleInvalid="keep"
#     )
    
#     # Scale features to improve model performance
#     scaler = StandardScaler(
#         inputCol="features",
#         outputCol="scaled_features",
#         withStd=True,
#         withMean=True
#     )
    
#     # Define the logistic regression model
#     lr = LogisticRegression(
#         featuresCol="scaled_features",
#         labelCol="Cancelled",
#         maxIter=10
#     )
    
#     # Combine all stages into a single pipeline
#     return Pipeline(stages=[
#         origin_indexer,
#         dest_indexer,
#         assembler,
#         scaler,
#         lr
#     ])

# def prepare_prediction_data(origin, dest, date, time):
#     """
#     Prepare a single row of data for prediction.
#     This function ensures the input data matches the training data structure.
#     """
#     # Create a schema that matches our processed training data
#     schema = StructType([
#         StructField("Origin", StringType(), True),
#         StructField("Dest", StringType(), True),
#         StructField("Month", IntegerType(), True),
#         StructField("DayofMonth", IntegerType(), True),
#         StructField("CRSDepTime", IntegerType(), True),
#         StructField("Cancelled", DoubleType(), True)
#     ])
    
#     # Create the input data with the same structure as training data
#     input_data = spark.createDataFrame(
#         [(
#             origin,
#             dest,
#             date.month,
#             date.day,
#             int(f"{time.hour:02d}{time.minute:02d}"),
#             0.0  # Dummy value for Cancelled
#         )],
#         schema=schema
#     )
    
#     # Apply the same transformation as training data
#     return input_data.withColumn(
#         "CRSDepTime_hour",
#         hour(coalesce(col("CRSDepTime").cast("string").cast("timestamp"), 
#                      lit("00:00:00").cast("timestamp")))
#     )


# def get_trained_model(df):
#     try:
#         processed_df = prepare_training_data(df)
#         pipeline = create_pipeline()
#         return pipeline.fit(processed_df)
#     except Exception as e:
#         st.error(f"Error during model training: {str(e)}")
#         return None