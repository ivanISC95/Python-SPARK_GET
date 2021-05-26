import requests
import json
import sys
from pyspark.sql.types import BooleanType
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import count, lit, sum


import parser

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SPARK -> GET -> API") \
        .getOrCreate()

    def getDataFromApi():
        #url = "http://144.202.17.134:8000/obtenerData"
        url = "http://144.202.17.134:8000/obtenerData"
        response = requests.get(url)
        return response
    

    data = getDataFromApi()
    json_rdd = spark.sparkContext.parallelize([data.text])
    df = spark.read.json(json_rdd)
    


    print("AQUI \n")    
    filtradoH = df.select("humedadIV")    
    total = filtradoH.agg(F.sum(df["humedadIV"]))
    promedio = filtradoH.agg(F.mean(df["humedadIV"]))
    print("HUMEDAD: ")
    print("\n")
    print("PROMEDIO: ")
    print(promedio.show(truncate=False))

    print("\n")
    print("DATOS INFRARROJO")
    infrarrojoT = df.select("presencia")
    infrarrojoL = df.select("presencia").filter(df.presencia == 'Libre')
    infrarrojoO = df.select("presencia").filter(df.presencia == 'Ocupado')
    print("Total")
    print(infrarrojoT.count())
    print("Libre")
    print(infrarrojoL.count())
    print("Ocupado")
    print(infrarrojoO.count())
    

    spark.stop()