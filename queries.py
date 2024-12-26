# Header Dataset: Year,"Quarter","Month","DayofMonth","DayOfWeek","FlightDate","Reporting_Airline","DOT_ID_Reporting_Airline",
# "IATA_CODE_Reporting_Airline","Tail_Number","Flight_Number_Reporting_Airline","OriginAirportID","OriginAirportSeqID","OriginCityMarketID",
# "Origin","OriginCityName","OriginState","OriginStateFips","OriginStateName","OriginWac","DestAirportID","DestAirportSeqID",
# "DestCityMarketID","Dest","DestCityName","DestState","DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime","DepDelay",
# "DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime","ArrTime",
# "ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled","CancellationCode","Diverted","CRSElapsedTime",
# "ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay",
# "LateAircraftDelay","FirstDepTime","TotalAddGTime","LongestAddGTime","DivAirportLandings","DivReachedDest","DivActualElapsedTime",
# "DivArrDelay","DivDistance","Div1Airport","Div1AirportID","Div1AirportSeqID","Div1WheelsOn","Div1TotalGTime","Div1LongestGTime",
# "Div1WheelsOff","Div1TailNum","Div2Airport","Div2AirportID","Div2AirportSeqID","Div2WheelsOn","Div2TotalGTime","Div2LongestGTime",
# "Div2WheelsOff","Div2TailNum","Div3Airport","Div3AirportID","Div3AirportSeqID","Div3WheelsOn","Div3TotalGTime","Div3LongestGTime",
# "Div3WheelsOff","Div3TailNum","Div4Airport","Div4AirportID","Div4AirportSeqID","Div4WheelsOn","Div4TotalGTime","Div4LongestGTime",
# "Div4WheelsOff","Div4TailNum","Div5Airport","Div5AirportID","Div5AirportSeqID","Div5WheelsOn","Div5TotalGTime","Div5LongestGTime",
# "Div5WheelsOff","Div5TailNum",


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, sum, asc, desc, when,avg,expr
import time
from geopy import Nominatim
import pandas as pd
from pyspark.sql.functions import round as pyspark_round


# Funzione per creare una sessione Spark
# def get_spark_session_TUTTO():
#     return SparkSession.builder \
#         .appName("Progetto BigData") \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.executor.memory", "4g") \
#         .getOrCreate()


# spark = get_spark_session_TUTTO()
# spark.sparkContext.setLogLevel("OFF")

# folder_path = r"C:\Users\xdomy\Desktop\Università\MAGISTRALE\1° Anno 1° Semestre\Modelli e Tecniche per Big Data\Progetto Voli\Dataset Voli"


#CODICE GZ
spark = SparkSession.builder.appName("Progetto BigData").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

folder_path = r"C:\Users\giuse\Desktop\UNIVERSITA'\MAGISTRALE\1° ANNO\1° SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\DATI"
file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]

#df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).drop("_c109").cache()
df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).limit(4000000).drop("_c109").cache()


#df = df.withColumn("FlightDate", to_date(col("FlightDate"), "yyyy-MM-dd"))
#df.select("FlightDate").show(5, truncate=False)

#EVENTUALMENTE QUA FARE CASTING SE VOGLIAMO RISULTATI TIPO RITARDI NON COME DOUBLE
# df = df.withColumn("ArrDelayMinutes", df["ArrDelayMinutes"].cast(IntegerType()))
# df.printSchema()




#---------------------------------------------QUERY PER VERIFICARE SE IL DATASET HA BISOGNO DI PREPROCESSING---------------------------------------------

coordinateAeroporti = "C:/Users/giuse/Desktop/UNIVERSITA'/MAGISTRALE/1° ANNO/1° SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/coordinates.csv"
# coordinateAeroporti = "C:/Users/xdomy/Desktop/Università/MAGISTRALE/1° Anno 1° Semestre/Modelli e Tecniche per Big Data/Progetto Voli/ProgettoBigData/coordinates.csv"

# Returns the coordinates for a given airport
def coordinates():
    locator = Nominatim(user_agent="myNewGeocoder")
    righe_filtrate=df.dropDuplicates(["Origin", "Dest"]).collect()
    for riga in righe_filtrate:
        time.sleep(1)
        # Coordinates for Origin
        found = locator.geocode(riga["Origin"] + " Airport "+ riga["OriginCityName"], timeout=None)
        if found is None:
            time.sleep(1)
            found = locator.geocode(riga["OriginCityName"], timeout=None)
        result = [found.latitude, found.longitude]
        dfRes = pd.DataFrame({'OriginLatitude': [result[0]], 'OriginLongitude': [result[1]]})
    return dfRes


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
    tratte_filtrate=df.filter((col("ArrDelayMinutes").isNotNull())& (col("DepDelayMinutes").isNotNull()))
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(desc("TotaleMinutiRitardo")).limit(10).show()
    return tratte

#MOSTRA SOLO LE PRIME 10 TRATTE PER RITARDI ALLA PARTENZA 
def tratte_con_piu_ritardi_partenza():
    tratte_filtrate=df.filter((col("ArrDelayMinutes").isNotNull()) & (col("DepDelayMinutes").isNotNull()))
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(desc("MinutiRitardoPartenza")).limit(10).show()
    return tratte

#MOSTRA SOLO LE PRIME 10 TRATTE PER RITARDI ALLA PARTENZA 
def tratte_con_piu_ritardi_arrivo():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(desc("MinutiRitardoArrivo")).limit(10).show()
    return tratte

#TRATTE CON MENO RITARDI TOTALI, VALGONO DISCORSI FATTI PER TRATTE CON PIU RITARDI TOTALI (UN PO INUTILE CONSIDERANDO CHE MOLTI ARRIVANO PRECISI, MAGARI TOGLIERE)
def tratte_con_meno_ritardi_totali():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(asc("TotaleMinutiRitardo")).limit(10).show()
    return tratte

def tratte_con_meno_ritardi_partenza():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(asc("MinutiRitardoPartenza")).limit(10).show()
    return tratte

def tratte_con_meno_ritardi_arrivo():
    tratte_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    tratte= tratte_filtrate.groupBy("Origin","Dest","OriginCityName", "DestCityName").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(asc("MinutiRitardoArrivo")).limit(10).show()
    return tratte

def numero_voli_per_tratta(origin,dest):
    voli_filtrati = df.filter((col("Origin") == origin) & (col("Dest") == dest))
    numero_di_voli = voli_filtrati.count()
    print("Num voli:",numero_di_voli)
    return numero_di_voli

def aereo_piu_km_percorsi():
    aerei_filtrati=df.filter(col("Tail_Number").isNotNull())
    aereo=aerei_filtrati.groupBy("Tail_Number").agg(sum("Distance").alias("TotalDistance")).orderBy(col("TotalDistance").desc()).limit(1)
    return aereo

def velocita_media_totale():
    velocita_filtrate=df.filter((col("Distance").isNotNull()) & (col("AirTime").isNotNull()))
    velocita=velocita_filtrate.withColumn("AverageSpeed", col("Distance") / (col("AirTime") / 60)).select(avg("AverageSpeed").alias("AverageAircraftSpeed")).show()
    return velocita

def velocita_media_per_tratta():
    velocita_filtrate=df.filter((col("Distance").isNotNull()) & (col("AirTime").isNotNull()))
    velocita=velocita_filtrate.withColumn("AverageSpeed", col("Distance") / (col("AirTime") / 60)).groupBy("Origin", "Dest").agg(avg("AverageSpeed").alias("AverageSpeedPerRoute")).orderBy(col("AverageSpeedPerRoute").desc()).limit(10).show()
    return velocita

#ATTENZIONE: IN QUESTE QUERY STO RESTITUENDO I PRIMI 10
def stati_piu_visitati():
  stati_filtrati= df.filter(col("DestStateName").isNotNull())
  stati=stati_filtrati.groupBy("DestStateName").count().orderBy(col("count").desc()).limit(10)
  stati.show()
  return stati

def compagnie_piu_voli_fatti():
  compagnie_filtrate= df.filter(col("Reporting_Airline").isNotNull())
  compagnie=compagnie_filtrate.groupBy("Reporting_Airline").count().orderBy(col("count").desc()).limit(10)
  compagnie.show()
  return compagnie

def citta_piu_visitate():
  citta_filtrate= df.filter(col("DestCityName").isNotNull())
  citta=citta_filtrate.groupBy("DestCityName").count().orderBy(col("count").desc()).limit(10)
  citta.show()
  return citta 

def giorno_della_settimana_con_piu_voli():
    giorni_filtrati=df.filter(col("DayOfWeek").isNotNull())
    giorno=giorni_filtrati.groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1)
    giorno.show()
    return giorno

def totale_voli_cancellati():
    cancellati=df.filter(col("Cancelled")=="1.00").count()
    print(cancellati)
    return cancellati

def percentuale_voli_cancellati():
    cancellati=df.filter(col("Cancelled")=="1.00").count()
    perc=(cancellati*100)/df.count()
    print(perc)
    return perc

#QUI E IN QUELLA CON MENO FARE LA COSA EVENTUALMENTE CI COLLECT E PRENDI INDICE 0
def mese_con_piu_cancellati():
    mesi_filtrati= df.filter(col("Month").isNotNull())
    mesi=mesi_filtrati.groupBy("Month").count().orderBy(col("count").desc()).limit(1).show()
    return mesi

def mese_con_meno_cancellati():
    mesi_filtrati= df.filter(col("Month").isNotNull())
    mesi=mesi_filtrati.groupBy("Month").count().orderBy(col("count").asc()).limit(1).show()
    return mesi


def percentuale_voli_anticipo():
    voli_filtrati=df.filter(col("ArrDelayMinutes").isNotNull())
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")==0).count()*100)/voli_filtrati.count()
    return round(voli,2) #prime due cifre dopo virgola

def totale_voli_in_ritardo():
    voli_filtrati=df.filter(col("ArrDelayMinutes").isNotNull())
    voli= voli_filtrati.filter(col("ArrDelayMinutes")>0).count()
    print(voli)
    return voli

#QUA PRENDERE CEIL/FLOOR O MEGLIO LE PRIME DUE DOPO LA VIRGOLA?
def percentuale_voli_ritardo():
    voli_filtrati=df.filter(col("ArrDelayMinutes").isNotNull())
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")>0).count()*100)/voli_filtrati.count()
    return round(voli,2) #prime due cifre dopo virgola

#QUA PASSO SOLO CODICE AEROPORTO, EVENTUALMENTE CITTA?
def ritardi_medi_al_decollo_per_aeroporto(aeroporto):
    voli_filtrati=df.filter((col("DepDelayMinutes").isNotNull()) & (col("DepDelayMinutes")>0) & (col("Origin")==aeroporto))
    somma_ritardi= (voli_filtrati.agg(avg("DepDelayMinutes").alias("RitardoAlDecollo"))).show()
    return somma_ritardi

def totale_voli_da_aeroporto(aeroporto):
    aeroporti_filtrati=df.filter((col("Origin")==aeroporto)).count()
    print(aeroporti_filtrati)
    return aeroporti_filtrati

#OVVIAMENTE SI INTENDE CHE DOVEVANO PARTIRE DA QUELLO
def totale_voli_cancellati_aeroporto(aeroporto):
    cancellati=df.filter((col("Cancelled")==1.00)&(col("Origin")==aeroporto)).count()
    print(cancellati)
    return cancellati

def percentuale_voli_cancellati_aeroporto(aeroporto):
    cancellati=df.filter((col("Cancelled")==1.00)&(col("Origin")==aeroporto)).count()
    perc=cancellati*100/totale_voli_da_aeroporto(aeroporto)
    print(perc)
    return perc

def totale_voli_compagnia(compagniaAerea):
    totale=df.filter((col("Reporting_Airline").isNotNull()) & (col("Reporting_Airline")==compagniaAerea)).count()
    print(totale)
    return totale

#PARTE DA CODICE DI MOMI percentualeVoliInOrario()
def percentuale_voli_in_Orario_compagnia(compagniaAerea):
    totale_voli = totale_voli_compagnia(compagniaAerea)
    voli_in_orario_compagnia = df.filter((col("ArrDelayMinutes")==0)&(col("Reporting_Airline")==compagniaAerea)).count()
    percentuale = (voli_in_orario_compagnia / totale_voli) * 100
    print(f"La percentuale di voli in orario della compagnia selezionata è: {percentuale:.2f}%")
    return percentuale


def compagnia_con_piu_ritardi_totali():
    compagnia_filtrate=df.filter((col("ArrDelayMinutes").isNotNull())& (col("DepDelayMinutes").isNotNull()))
    compagnie= compagnia_filtrate.groupBy("Reporting_Airline").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(desc("TotaleMinutiRitardo")).limit(10).show()
    return compagnie

#MOSTRA SOLO LE PRIME 10 COMPAGNIE PER RITARDI ALLA PARTENZA 
def compagnia_con_piu_ritardi_partenza():
    compagnie_filtrate=df.filter((col("ArrDelayMinutes").isNotNull()) & (col("DepDelayMinutes").isNotNull()))
    compagnie= compagnie_filtrate.groupBy("Reporting_Airline").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(desc("MinutiRitardoPartenza")).limit(10).show()
    return compagnie

#MOSTRA SOLO LE PRIME 10 COMPAGNIE PER RITARDI ALLA PARTENZA 
def compagnia_con_piu_ritasrdi_arrivo():
    compagnie_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    compagnie= compagnie_filtrate.groupBy("Reporting_Airline").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(desc("MinutiRitardoArrivo")).limit(10).show()
    return compagnie

#COMPAFNIE CON MENO RITARDI TOTALI, VALGONO DISCORSI FATTI PER COMPAGNIE CON PIU RITARDI TOTALI (UN PO INUTILE CONSIDERANDO CHE MOLTI ARRIVANO PRECISI, MAGARI TOGLIERE)
def compagnia_con_meno_ritardi_totali():
    compagnie_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    compagnie= compagnie_filtrate.groupBy("Reporting_Airline").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
                                            sum("ArrDelayMinutes").alias("MinutiRitardoArrivo"))\
    .withColumn("TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")).orderBy(asc("TotaleMinutiRitardo")).limit(10).show()
    return compagnie

def compagnia_con_meno_ritardi_partenza():
    compagnie_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    compagnie= compagnie_filtrate.groupBy("Reporting_Airline").agg(sum("DepDelayMinutes").alias("MinutiRitardoPartenza")).orderBy(asc("MinutiRitardoPartenza")).limit(10).show()
    return compagnie

def compagnia_con_meno_ritardi_arrivo():
    compagnie_filtrate=df.filter(col("ArrDelayMinutes").isNotNull())
    compagnie= compagnie_filtrate.groupBy("Reporting_Airline").agg(sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")).orderBy(asc("MinutiRitardoArrivo")).limit(10).show()
    return compagnie

def numero_voli_per_compagnia(compagnia):
    voli_filtrati = df.filter((col("Reporting_Airline") ==compagnia))
    numero_di_voli = voli_filtrati.count()
    print("Num voli per la compagnia selezionata:",numero_di_voli)
    return numero_di_voli

#ATTENZIONE: IN QUESTE QUERY STO RESTITUENDO I PRIMI 10
def stati_piu_visitati_compagnia(compagnia):
  stati_filtrati= df.filter((col("DestStateName").isNotNull()) & (col("Reporting_Airline")==compagnia))
  stati=stati_filtrati.groupBy("DestStateName").count().orderBy(col("count").desc()).limit(10).show()
  return stati

def citta_piu_visitate_compagnia(compagnia):
  citta_filtrate= df.filter((col("DestCityName").isNotNull()) & (col("Reporting_Airline")==compagnia))
  citta=citta_filtrate.groupBy("DestCityName").count().orderBy(col("count").desc()).limit(10).show()
  return citta 

def giorno_della_settimana_con_piu_voli_compagnia(compagnia):
    giorni_filtrati=df.filter((col("DayOfWeek").isNotNull()) & (col("Reporting_Airline")==compagnia))
    giorno=giorni_filtrati.groupBy("DayOfWeek").count().orderBy(col("count").desc()).limit(1).show()
    return giorno

def totale_voli_cancellati_compagnia(compagnia):
    cancellati=df.filter((col("Cancelled")=="1.00") & (col("Reporting_Airline")==compagnia)).count()
    print(cancellati)
    return cancellati

def percentuale_voli_cancellati_compagnia(compagnia):
    cancellati=df.filter((col("Cancelled")=="1.00") & (col("Reporting_Airline")==compagnia)).count()
    perc=(cancellati*100)/df.count()
    print(perc)
    return perc

#QUI E IN QUELLA CON MENO FARE LA COSA EVENTUALMENTE CI COLLECT E PRENDI INDICE 0
def mese_con_piu_cancellati_compagnia(compagnia):
    mesi_filtrati= df.filter((col("Month").isNotNull()) & (col("Reporting_Airline")==compagnia))
    mesi=mesi_filtrati.groupBy("Month").count().orderBy(col("count").desc()).limit(1).show()
    return mesi

def mese_con_meno_cancellati_compagnia(compagnia):
    mesi_filtrati= df.filter((col("Month").isNotNull()) & (col("Reporting_Airline")==compagnia))
    mesi=mesi_filtrati.groupBy("Month").count().orderBy(col("count").asc()).limit(1).show()
    return mesi

#QUA PRENDERE CEIL/FLOOR O MEGLIO LE PRIME DUE DOPO LA VIRGOLA?
def percentuale_voli_anticipo_compagnia(compagnia):
    voli_filtrati=df.filter((col("ArrDelayMinutes").isNotNull()) & (col("Reporting_Airline")==compagnia))
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")==0).count()*100)/voli_filtrati.count()
    print(voli)
    return voli

def totale_voli_in_ritardo_compagnia(compagnia):
    voli_filtrati=df.filter((col("ArrDelayMinutes").isNotNull()) & (col("Reporting_Airline")==compagnia))
    voli= voli_filtrati.filter(col("ArrDelayMinutes")>0).count()
    print(voli)
    return voli

#QUA PRENDERE CEIL/FLOOR O MEGLIO LE PRIME DUE DOPO LA VIRGOLA?
def percentuale_voli_ritardo_compagnia(compagnia):
    voli_filtrati=df.filter((col("ArrDelayMinutes").isNotNull()) & (col("Reporting_Airline")==compagnia))
    voli= (voli_filtrati.filter(col("ArrDelayMinutes")>0).count()*100)/voli_filtrati.count()
    print(voli)
    return voli

#QUA PASSO SOLO CODICE AEROPORTO, EVENTUALMENTE CITTA?
def ritardi_medi_al_decollo_per_compagnia(compagnia):
    voli_filtrati=df.filter((col("DepDelayMinutes").isNotNull()) & (col("DepDelayMinutes")>0) & (col("Reporting_Airline")==compagnia))
    somma_ritardi= (voli_filtrati.agg(avg("DepDelayMinutes").alias("RitardoAlDecollo"))).show()
    return somma_ritardi

def totale_voli_compagnia(compagnia):
    compagnie_filtrate=df.filter((col("Reporting_Airline")==compagnia)).count()
    print(compagnie_filtrate)
    return compagnie_filtrate

#OVVIAMENTE SI INTENDE CHE DOVEVANO PARTIRE DA QUELLO
def totale_voli_cancellati_compagnia(compagnia):
    cancellati=df.filter((col("Cancelled")==1.00)&(col("Reporting_Airline")==compagnia)).count()
    print(cancellati)
    return cancellati

def percentuale_voli_cancellati_compagnia(compagnia):
    cancellati=df.filter((col("Cancelled")==1.00)&(col("Reporting_Airline")==compagnia)).count()
    perc=cancellati*100/totale_voli_compagnia(compagnia)
    print(perc)
    return perc

def percentuale_voli_deviati():
    voli_filtrati = df.filter(col("Diverted") == 1).count()
    percentuale_voli_deviati = (voli_filtrati / df.count()) * 100
    print(f"Percentuale di voli deviati: {percentuale_voli_deviati:.2f}%")
    return percentuale_voli_deviati

def percentuale_voli_deviati_compagnia(compagnia):
    voli_filtrati = df.filter((col("Diverted") == 1) & (col("Reporting_Airline")==compagnia)).count()
    percentuale_voli_deviati = (voli_filtrati / numero_voli_per_compagnia(compagnia)) * 100
    print(f"Percentuale di voli deviati: {percentuale_voli_deviati:.2f}%")
    return percentuale_voli_deviati

def percentuale_voli_deviati_aeroporto(aeroporto):
    voli_filtrati = df.filter((col("Diverted") == 1) & (col("Origin")==aeroporto)).count()
    percentuale_voli_deviati = (voli_filtrati / totale_voli_da_aeroporto(aeroporto)) * 100
    print(f"Percentuale di voli deviati: {percentuale_voli_deviati:.2f}%")
    return percentuale_voli_deviati

def media_del_tempo_di_volo_compagnia(compagnia):
    voli_filtrati=df.filter((col("AirTime").isNotNull()) & (col("Reporting_Airline")==compagnia))
    somma_ritardi= (voli_filtrati.agg(avg("AirTime").alias("TempoMedioVolo"))).show()
    return somma_ritardi

#VEDERE SE RESTITUISCE STESSO RISULTATO DI QUELLA PER MESE
def numero_voli_periodo(data_inizio, data_fine=None):
    # Se data_fine è fornito, considerare un periodo
    if data_fine:
        voli_filtrati = df.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    else:
        #li cerco da li in poi
        voli_filtrati = df.filter(col("FlightDate") >= data_inizio)
    print(voli_filtrati.count())
    return voli_filtrati.count()


def percentuale_in_orario_periodo(data_inizio, data_fine):
    voli_filtrati = df.filter((col("ArrDelayMinutes").isNotNull()) & (col("FlightDate").isNotNull()) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    #mi prendo i totali per la percentuale, non faccio count direttamente su voli filtrati oppure poi non posso filtrare solo i cancellati
    voli_totali=voli_filtrati.count()
    in_orario = voli_filtrati.filter(col("ArrDelayMinutes") == 0).count()
    percentuale = (in_orario /voli_totali ) * 100 if voli_totali > 0 else 0
    print(percentuale)
    return percentuale

def percentuale_in_ritardo_periodo(data_inizio, data_fine):
    voli_filtrati = df.filter((col("ArrDelayMinutes").isNotNull()) &(col("FlightDate").isNotNull()) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    #mi prendo i totali per la percentuale, non faccio count direttamente su voli filtrati oppure poi non posso filtrare solo i cancellati
    voli_totali=voli_filtrati.count()
    in_ritardo = voli_filtrati.filter(col("ArrDelayMinutes") > 0).count()
    percentuale = (in_ritardo / voli_totali) * 100 if voli_totali > 0 else 0
    print(percentuale)
    return percentuale

def percentuale_cancellati_periodo(data_inizio, data_fine):
    voli_filtrati = df.filter((col("Cancelled").isNotNull()) & (col("FlightDate").isNotNull()) & (col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    #mi prendo i totali per la percentuale, non faccio count direttamente su voli filtrati oppure poi non posso filtrare solo i cancellati
    voli_totali=voli_filtrati.count()
    cancellati = voli_filtrati.filter(col("Cancelled") == 1).count()
    percentuale = (cancellati / voli_totali) * 100 if voli_totali > 0 else 0
    print(percentuale)
    return percentuale

def compagnia_piu_voli_nel_periodo(data_inizio,data_fine):
    voli_filtrati = df.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    risultato = (voli_filtrati.groupBy("Reporting_Airline")
              .agg(count("*").alias("voli_nel_periodo"))
              .orderBy(col("voli_nel_periodo").desc())).limit(1).show()
    return risultato


def aerei_con_ritardo_specificato_partenza_e_arrivo(rit_partenza, rit_arrivo, aeroporto=None, compagnia=None):
    voli_filtrati = df.filter((col("DepDelayMinutes") >= rit_partenza) & (col("ArrDelayMinutes") >= rit_arrivo))    
    if aeroporto:
        voli_filtrati = voli_filtrati.filter((col("Origin") == aeroporto) | (col("Dest") == aeroporto))
    if compagnia:
        voli_filtrati = voli_filtrati.filter(col("Reporting_Airline") == compagnia)    
    result = voli_filtrati.select("Tail_Number").distinct().count()
    print(result)
    return result

def percentuali_cause_ritardo(filtro_compagnia=None, causa_specifica=None, data_inizio=None, data_fine=None,stato=None):
    # Filtro opzionale per compagnia
    df_filtrato = df
    if stato:
        df_filtrato=df_filtrato.filter((col("OriginStateName")==stato) | (col("DestStateName")==stato))
    if filtro_compagnia:
        df_filtrato = df_filtrato.filter(col("Reporting_Airline") == filtro_compagnia)
    
    # Filtro opzionale per periodo
    if data_inizio and data_fine:
        df_filtrato = df_filtrato.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    
    # Sostituisco i valori nulli nelle colonne di ritardo con 0
    df_filtrato = df_filtrato.fillna(0, subset=["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"])
    
    # Somma dei minuti di ritardo per ogni causa
    ritardi_cause = df_filtrato.select(
        ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
    ).agg(
        sum("CarrierDelay").alias("CarrierDelay"),
        sum("WeatherDelay").alias("WeatherDelay"),
        sum("NASDelay").alias("NASDelay"),
        sum("SecurityDelay").alias("SecurityDelay"),
        sum("LateAircraftDelay").alias("LateAircraftDelay")
    )
    # Somma di tutte le cause per ottenere il totale complessivo dei ritardi
    ritardi_somma = ritardi_cause.select(
        (col("CarrierDelay") +
         col("WeatherDelay") +
         col("NASDelay") +
         col("SecurityDelay") +
         col("LateAircraftDelay")).alias("TotaleRitardo")
    ).collect()[0]["TotaleRitardo"]  
    # Calcolo percentuali
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
    ritardi_percentuali.show()
    return ritardi_percentuali

def ritardo_medio_per_stagione(aeroporto=None,compagnia=None):
    # Definisco le stagioni
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
    # Creo una colonna per le stagioni
    df1 = df_filtrato.withColumn(
        "Stagione",
        when(col("Month").isin(season_mapping["Inverno"]), "Inverno")
         .when(col("Month").isin(season_mapping["Primavera"]), "Primavera")
         .when(col("Month").isin(season_mapping["Estate"]), "Estate")
         .when(col("Month").isin(season_mapping["Autunno"]), "Autunno")
    )
    # Calcolo il ritardo medio per stagione
    avg_delays = df1.groupBy("Stagione").agg(avg("ArrDelayMinutes").alias("AvgDelay"))
    # Ordino le stagioni in modo che se le vogliamo plottare sono ordinate
    result = avg_delays.orderBy(expr(
        "case Stagione when 'Inverno' then 1 when 'Primavera' then 2 when 'Estate' then 3 when 'Autunno' then 4 end"
    ))
    print(result.show())
    return result

def ritardo_medio_partenza_stato(stato):
    voli_filtrati = df.filter(col("OriginStateName")==stato).agg(avg("DepDelay").alias("AvgDepartureDelay")).collect()
    print(voli_filtrati[0]["AvgDepartureDelay"])
    return voli_filtrati[0]["AvgDepartureDelay"]


def stati_con_maggiore_increm_ritardo_inverno_rispetto_estate():
    # FiltrO per mesi invernali 
    mesi_invernali = [12, 1, 2]
    ritardi_invernali = df.filter(col("Month").isin(mesi_invernali)) \
                      .groupBy("OriginStateName") \
                      .agg(avg("ArrDelayMinutes").alias("WinterAvgDelay"))
    
    # Filtro per mesi estivi 
    mesi_estivi = [6, 7, 8]
    ritardi_estivi = df.filter(col("Month").isin(mesi_estivi)) \
                      .groupBy("OriginStateName") \
                      .agg(avg("ArrDelayMinutes").alias("SummerAvgDelay"))
    
    # Unione dei dati invernali ed estivi
    confronto_ritardi = ritardi_invernali.join(ritardi_estivi, on="OriginStateName", how="inner")
    # Calcolo dell'incremento del ritardo medio
    confronto_ritardi = confronto_ritardi.withColumn(
        "DelayIncrease", col("WinterAvgDelay") - col("SummerAvgDelay")
    )
    # Ordinamento per incremento decrescente
    result = confronto_ritardi.orderBy(col("DelayIncrease").desc()).show()
    return result


#AGGIUSTA LA RETURN A QUESTE 3 E MAGARI FAI RESTITUIRE UNO SOLO ALLA DUE SOTTO
def stati_con_minore_ritardo_arrivo():
    stati_ritardo = df.filter(col("ArrDelayMinutes").isNotNull()) \
        .groupBy("DestStateName") \
        .agg(avg("ArrDelayMinutes").alias("MediaRitardoArrivo")) \
        .orderBy(asc("MediaRitardoArrivo")) \
        .limit(10)
    stati_ritardo.select("DestStateName", "MediaRitardoArrivo").show(truncate=False)
    return stati_ritardo

def tratta_piu_comune_da_stato(stato):
    tratta_comune = df.filter(col("OriginStateName") == stato) \
        .groupBy("OriginCityName", "DestCityName") \
        .count() \
        .orderBy(desc("count")) \
        .limit(1)
    tratta_comune.select("OriginCityName", "DestCityName", "count").show(truncate=False)
    return tratta_comune

#FORSE INUTILE PERCHÈ IL CONTRARIO DI QUELLA COME ORIGINE SOPRA
def tratta_piu_comune_per_stato(stato):
    tratta_comune = df.filter(col("DestStateName") == stato) \
        .groupBy("OriginCityName", "DestCityName") \
        .count() \
        .orderBy(desc("count")) \
        .limit(1)
    tratta_comune.select("OriginCityName", "DestCityName", "count").show(truncate=False)
    return tratta_comune


def stati_maggiore_traffico_aereo():
    traffico = df.groupBy("OriginStateName").agg(count("*").alias("Partenze")) \
        .join(
            df.groupBy("DestStateName").agg(count("*").alias("Arrivi")),
            col("OriginStateName") == col("DestStateName"),
            "outer"
        ) \
        .withColumn("TotaleTraffico", col("Partenze") + col("Arrivi")) \
        .orderBy(desc("TotaleTraffico")) \
        .select("OriginStateName", "TotaleTraffico") \
        .limit(10)
    traffico.show(truncate=False)
    return traffico

def stati_maggiore_efficienza():
    efficienza = df.filter((col("ArrDelayMinutes").isNotNull()) & (col("DepDelayMinutes").isNotNull())) \
        .groupBy("DestStateName") \
        .agg(
            (sum("ArrDelayMinutes") + sum("DepDelayMinutes")).alias("TotaleRitardi"),
            count("*").alias("TotaleVoli")
        ) \
        .withColumn("Efficienza", col("TotaleVoli")*100 / col("TotaleRitardi")) \
        .orderBy(desc("Efficienza")) \
        .limit(10)
    efficienza.select("DestStateName", "Efficienza").show(truncate=False)
    return efficienza


#---------------------------------------------QUERY GENERALI MOMI---------------------------------------------


#------------------------QUERY SCHERMATA HOME#------------------------

# Giorno della settimana con più voli
def giornoNumMaxVoli():
    tratte_filtrate=df.filter(col("DayOfWeek").isNotNull())
    voli_per_giorno = tratte_filtrate.groupBy("DayOfWeek").agg(count("*").alias("NumeroVoli")) # Raggruppa per il giorno della settimana e conta il numero di voli
    giorno = voli_per_giorno.orderBy(col("NumeroVoli").desc()).limit(1).show(truncate=True) # Ordina i risultati per il numero di voli in ordine decrescente e prende il primo, .collect() restituisce una lista di righe in formato python
    #giorno_con_piu_voli = giorno[0]["DayOfWeek"]
    #return giorno_con_piu_voli  mettiamo queste due righe quando dovremo ritornare il numero, inserendo quindi la collect nella variabile giorno, cosi mostra solo a grafico
    return giorno

# 10 aeroporti con il maggior traffico come destinazione  RENDERE GENERALE
def dieciAeroportiMaggiorTrafficoDestinazione():
    tratte_filtrate = df.filter(col("Dest").isNotNull())
    aeroporti_destinazione = tratte_filtrate.groupBy("Dest").agg(count("*").alias("NumeroVoli")) # Raggruppa per aeroporto di destinazione e conta i voli
    dieci_aeroporti = aeroporti_destinazione.orderBy(col("NumeroVoli").desc()).limit(10) # Ordina per numero di voli in ordine decrescente e seleziona i primi 10
    dieci_aeroporti.show(truncate=True) # Mostra i risultati in una tabella
    return dieci_aeroporti # Ritorna il DataFrame dei 10 aeroporti (per ulteriori elaborazioni, se necessario)

# Rotte (origine-destinazione) più comuni
def rottePiuComuni():
    tratte_filtrate = df.filter(col("Origin").isNotNull() & col("Dest").isNotNull())
    rotte = tratte_filtrate.groupBy("Origin", "Dest").agg(count("*").alias("NumeroVoli")) # Raggruppa per rotta e conta i voli
    rotte_piu_comuni = rotte.orderBy(col("NumeroVoli").desc()).limit(10) # Ordina per numero di voli in ordine decrescente e seleziona i primi 10
    rotte_piu_comuni.show(truncate=True) # Mostra i risultati in una tabella
    return rotte_piu_comuni # Ritorna il DataFrame delle 10 rotte più comuni (per ulteriori elaborazioni, se necessario)

# Compagnia con più voli cancellati e percentuali cause
def compagniaPiuVoliCancellati():
    # Filtra i voli cancellati (Cancelled == 1)
    voli_cancellati = df.filter(col("Cancelled") == 1)
    # Raggruppa per compagnia aerea e conta i voli cancellati
    cancellati_per_compagnia = voli_cancellati.groupBy("Reporting_Airline").agg(count("*").alias("NumeroVoliCancellati"))
    # Trova la compagnia con il maggior numero di voli cancellati
    compagnia_max_cancellazioni = cancellati_per_compagnia.orderBy(col("NumeroVoliCancellati").desc()).limit(1).collect()
    if compagnia_max_cancellazioni:
        compagnia = compagnia_max_cancellazioni[0]["Reporting_Airline"]
        totale_cancellati = compagnia_max_cancellazioni[0]["NumeroVoliCancellati"]
        # Filtra i voli cancellati della compagnia con più cancellazioni
        cancellazioni_compagnia = voli_cancellati.filter(col("Reporting_Airline") == compagnia)
        # Calcola la percentuale per ciascuna causa
        percentuali_cause = (
            cancellazioni_compagnia.groupBy("CancellationCode")
            .agg((count("*") / totale_cancellati * 100).alias("Percentuale"))
            .orderBy("CancellationCode")
        )
        # Mostra i risultati
        #print(f"La compagnia con più voli cancellati è: {compagnia} con {totale_cancellati} voli cancellati.")
        percentuali_cause.show(truncate=True)
        return compagnia, percentuali_cause.collect()
    else:
        print("Nessun volo cancellato trovato nel dataset.")
        return None, None

# Compagnia aerea con più km percorsi
def compagniaPiuKmPercorsi():
    voli_con_tail_number = df.filter(col("Tail_Number").isNotNull())
    km_percorsi_per_compagnia = voli_con_tail_number.groupBy("Reporting_Airline").agg(sum("Distance").alias("TotaleKm"))
    compagnia_max_km = km_percorsi_per_compagnia.orderBy(col("TotaleKm").desc()).limit(10)
    compagnia_max_km.show()
    return compagnia_max_km

# Stato più visitato
def statoPiuVisitato():
    tratte_filtrate = df.filter(col("DestState").isNotNull())
    stati = tratte_filtrate.groupBy("DestState").agg(count("*").alias("NumeroVoli"))
    stato_piu_visitato = stati.orderBy(col("NumeroVoli").desc()).limit(1).show(truncate=True)
    return stato_piu_visitato

# Stati con il minor ritardo medio
def statiMinRitardoMedio():
    tratte_filtrate = df.filter(col("ArrDelayMinutes").isNotNull())
    ritardo_medio_per_stato = tratte_filtrate.groupBy("DestStateName").agg(mean("ArrDelayMinutes").alias("RitardoMedio"))
    stati_min_ritardo = ritardo_medio_per_stato.orderBy(col("RitardoMedio")).limit(10)
    stati_min_ritardo = stati_min_ritardo.withColumn('RitardoMedio', pyspark_round(col('RitardoMedio'), 2))
    stati_min_ritardo.show(truncate=True)
    return stati_min_ritardo


#------------------------QUERY VOLI------------------------

# Numero totale di voli per mese
def totaleVoliPerMese():
    voli_per_mese = df.groupBy("Month").agg(count("*").alias("NumeroVoli")).orderBy("Month")
    voli_per_mese.show(truncate=True)
    return voli_per_mese

# Trovare il volo con la distanza massima percorsa (E MINIMA) FATTO DA GZ -> volo_distanza_max() e volo_distanza_min()

# Calcolare la percentuale di voli in orario (ritardo ≤ 0 minuti)
def percentualeVoliInOrario():
    # Conta il numero totale di voli nel dataset
    totale_voli = df.count()
    # Filtra i voli che sono arrivati in orario (ritardo ≤ 0 minuti)
    voli_in_orario = df.filter(col("ArrDelay") <= 0).count()
    # Calcola la percentuale
    percentuale = (voli_in_orario / totale_voli) * 100
    print(f"La percentuale di voli in orario è: {percentuale:.2f}%")
    return percentuale

# Identificare le rotte (origine-destinazione) più comuni. -> rottePiuComuni()

# Analizzare le cancellazioni di voli e collegarle al giorno della settimana ???

# Sceglie aereo e mostra le tratte di quell'aereo
def mostraTratteAereo(tail_number):
    # Filtra le tratte per il Tail_Number specificato
    tratte_aereo = df.filter(col("Tail_Number") == tail_number)
    if tratte_aereo.count() > 0:
        # Seleziona le tratte uniche (Origin, Dest)
        tratte_uniche = tratte_aereo.select("Origin", "Dest").distinct()
        # Mostra i risultati
        print(f"Tratte uniche per l'aereo con Tail_Number '{tail_number}':")
        tratte_uniche.show(truncate=False)
        # Restituisce il DataFrame delle tratte uniche
        return tratte_uniche
    else:
        print(f"Nessuna tratta trovata per l'aereo con Tail_Number '{tail_number}'.")
        return None

# Numero di voli che hanno subito un ritardo al decollo ma sono atterrati in anticipo
def voliRitardoDecolloArrivoAnticipo():
    voli_filtrati = df.filter((col("DepDelay") > 0) & (col("ArrDelay") < 0))
    numero_voli = voli_filtrati.count()
    print(f"Il numero di voli che hanno subito un ritardo al decollo ma sono atterrati in anticipo è: {numero_voli}")
    return numero_voli

#------------------------QUERY VOLI------------------------

# Calcolare la media dei ritardi alla partenza per aeroporto di origine (se non si passa nessun parametro alla funzione mostra la media per tutti gli aereoporti diversi)
def mediaRitardiPartenza(aeroporto=None): # =None significa che si può non specificare
    if aeroporto:
        ritardi = df.filter(col("Origin") == aeroporto).select(mean("DepDelay").alias("MediaRitardo"))
        risultato = ritardi.collect()
        if risultato:
            media = risultato[0]["MediaRitardo"]
            print(f"La media dei ritardi alla partenza per l'aeroporto '{aeroporto}' è: {media:.2f} minuti.")
            return media
        else:
            print(f"Nessun dato disponibile per l'aeroporto '{aeroporto}'.")
            return None
    else:
        media_per_aeroporto = (
            df.groupBy("Origin")
            .agg(mean("DepDelay").alias("MediaRitardo"))
            .orderBy("Origin")
        )
        print("Media dei ritardi alla partenza per tutti gli aeroporti:")
        media_per_aeroporto.show(truncate=False)
        return media_per_aeroporto


# Percentuale di voli in orario che arrivano in quello aeroporto (ritardo ≤ 0 minuti)
def percentualeVoliInOrarioArrivi(aeroporto=None):
    if aeroporto:
        voli_arrivo = df.filter(col("Dest") == aeroporto)
        totale_voli_arrivo = voli_arrivo.count()    
        if totale_voli_arrivo > 0:
            voli_in_orario = voli_arrivo.filter(col("ArrDelay") <= 0).count()
            percentuale = (voli_in_orario / totale_voli_arrivo) * 100
            print(f"La percentuale di voli in orario che arrivano all'aeroporto '{aeroporto}' è: {percentuale:.2f}%")
            return percentuale
        else:
            print(f"Nessun volo in arrivo registrato per l'aeroporto '{aeroporto}'.")
            return None
    else:
        print("Specificare un aeroporto come parametro.")
        return None