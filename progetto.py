from pyspark.sql import SparkSession
from pyspark.sql.functions import *#col, when, isnan, count, regexp_replace, mean, to_date,desc,sum 
from pyspark.sql.types import IntegerType
import os

spark = SparkSession.builder.appName("Progetto BigData").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# Lista dei file CSV
folder_path = r"C:\Users\giuse\Desktop\UNIVERSITA'\MAGISTRALE\1° ANNO\1° SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\DATI"
file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

# Caricamento dei file CSV in Spark
df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).limit(5000000).drop("_c109").cache()  #eliminiamo ultima colonna che inferiva erroneamente per come era fatto il dataset --limito a 5mln cosi va 

#EVENTUALMENTE QUA FARE CASTING SE VOGLIAMO RISULTATI TIPO RITARDI NON COME DOUBLE
#df = df.withColumn("ArrDelayMinutes", df["ArrDelayMinutes"].cast(IntegerType()))
#df.printSchema()

#---------------------------------------------QUERY PER VERIFICARE SE IL DATASET HA BISOGNO DI PREPROCESSING---------------------------------------------

def conta_righe_totali():
    return df.count()

def conta_righe_duplicate():
    df.groupBy(df.columns).count().filter("count > 1").count()  #0 duplicate fino a 5mln di righe

# Elenco delle colonne e condizioni per il controllo delle anomalie 
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
        print(f"Analizzando la colonna: {column}")
        anomalies = df.filter(eval(condition))  # eval valuta la condizione scritta come stringa
        ##anomalies.show(truncate=False)
        print(f"Numero di anomalie in {column}: {anomalies.count()}")




#---------------------------------------------QUERY GENERALI---------------------------------------------

def volo_distanza_max(): #posso cambiare colonne da restituire 
    volo=df.orderBy(col("Distance").desc()).limit(1)
    volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance").show(truncate=True)
    return volo

def volo_distanza_min(): #posso cambiare colonne da restituire 
    volo=df.orderBy(col("Distance").asc()).limit(1)
    volo.select("Origin", "Dest", "OriginCityName", "DestCityName","Distance").show(truncate=True)
    return volo


#NON VIENE FATTO FILTER SU COLONNE IN CUI DEPDELAY E ARRDELAY NON SIANO NULL PERCHE IN AUTOMATICO VENGONO SCARTATE
#qui nella group by uso anche origin city name e destcity name, nella select finale se voglio restituire altro devo fare una join finale con colonne 
#da aggiungere per esempio per distanza:
# tratte_completate = tratte.join( df.select("Origin", "Dest", "OriginCityName", "DestCityName", "Distance").distinct(),on=["Origin", "Dest"],how="left")
#QUI PRENDO SOLO LE PRIME 10!!
#ATTENZIONE: È IL RITARDO DELLA TRATTA NELL'INTERO ANNO, SIA ALLA PARTENZA CHE ALL'ARRIVO !!
def tratte_con_piu_ritardi_totali():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(desc("TotaleMinutiRitardo")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","TotaleMinutiRitardo").show(truncate=True)
    return tratte

#MOSTRA SOLO LE PRIME 10 TRATTE PER RITARDI ALLA PARTENZA 
def tratte_con_piu_ritardi_partenza():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(desc("MinutiRitardoPartenza")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","MinutiRitardoPartenza").show(truncate=True)
    return tratte

#MOSTRA SOLO LE PRIME 10 TRATTE PER RITARDI ALLA PARTENZA 
def tratte_con_piu_ritardi_arrivo():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(desc("MinutiRitardoArrivo")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","MinutiRitardoArrivo").show(truncate=True)
    return tratte

#TRATTE CON MENO RITARDI TOTALI, VALGONO DISCORSI FATTI PER TRATTE CON PIU RITARDI TOTALI (UN PO INUTILE CONSIDERANDO CHE MOLTI ARRIVANO PRECISI, MAGARI TOGLIERE)
def tratte_con_meno_ritardi_totali():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(asc("TotaleMinutiRitardo")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","TotaleMinutiRitardo").show(truncate=True)
    return tratte

def tratte_con_meno_ritardi_partenza():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(asc("MinutiRitardoPartenza")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","MinutiRitardoPartenza").show(truncate=True)
    return tratte


def tratte_con_meno_ritardi_arrivo():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(asc("MinutiRitardoArrivo")).limit(10)
    tratte.select("Origin", "Dest","OriginCityName", "DestCityName","MinutiRitardoArrivo").show(truncate=True)
    return tratte

def numero_voli_per_tratta(origin,dest):
    voli_filtrati = df.filter((col("Origin") == origin) & (col("Dest") == dest))
    numero_di_voli = voli_filtrati.count()
    return numero_di_voli

def aereo_piu_km_percorsi():
    aerei_filtrati=df.filter(col("Tail_Number").isNotNull())
    aereo=aerei_filtrati.groupBy("Tail_Number").agg(sum("Distance").alias("TotalDistance")).orderBy(col("TotalDistance").desc()).limit(1).show()
    return aereo

#volo_distanza_max()
#volo_distanza_min()
#tratte_con_meno_ritardi_totali()
#tratte_con_meno_ritardi_partenza()
#tratte_con_meno_ritardi_arrivo()
#numero_voli_per_tratta("LAX","SFO")
aereo_piu_km_percorsi()