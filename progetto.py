
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, regexp_replace, mean
import os

spark = SparkSession.builder.appName("Progetto BigData").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# Lista dei file CSV
folder_path = r"C:\Users\giuse\Desktop\UNIVERSITA'\MAGISTRALE\1° ANNO\1° SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\DATI"
file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]


path1 = "C:/Users/giuse/Desktop/UNIVERSITA'/MAGISTRALE/1° ANNO/1° SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/DATI/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv"

# Caricamento dei file CSV in Spark
#df = spark.read.options(delimiter=',').csv(file_list, header=True, inferSchema=True).cache()
df = spark.read.options(delimiter=',').csv(path1, header=True, inferSchema=True).cache()
df.printSchema()
print("TOTALE RIGHE",df.count())

def conta_righe_totali():
    return df.count()

