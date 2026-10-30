import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = '/home/vboxuser/data/Road_Accident_Data.csv'
SALIDA = '/home/vboxuser/resultados/hist_hora'

# nombre interno -> columna real del CSV (ajustar si es necesario)
MAPA = {
    'fecha': 'Accident Date',
    'hora': 'Time',
    'latitud': 'Latitude',
    'longitud': 'Longitude',
    'num_victimas': 'Number_of_Casualties',
    'num_vehiculos': 'Number_of_Vehicles',
    'tipo_vehiculo': 'Vehicle_Type',
    'clima': 'Weather_Conditions',
}

spark = (SparkSession.builder.appName('AccidentesHistorico')
         .config('spark.sql.shuffle.partitions', '4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

raw = spark.read.option('header', True).csv(RUTA)
faltantes = [c for c in MAPA.values() if c not in raw.columns]
if faltantes:
    print('Columnas no encontradas:', faltantes)
    print('Columnas disponibles:', raw.columns)
    sys.exit(1)

df = raw.select(*[F.col(c).alias(a) for a, c in MAPA.items()])
df = (df
      .withColumn('latitud', F.col('latitud').cast('double'))
      .withColumn('longitud', F.col('longitud').cast('double'))
      .withColumn('num_victimas', F.col('num_victimas').cast('int'))
      .withColumn('num_vehiculos', F.col('num_vehiculos').cast('int'))
      .withColumn('hora_dia', F.split(F.col('hora'), ':').getItem(0).cast('int'))
      .withColumn('zona', F.concat_ws(',', F.round('latitud', 2).cast('string'),
                                     F.round('longitud', 2).cast('string')))
      .dropna(subset=['latitud', 'longitud', 'hora_dia'])
      .fillna({'num_victimas': 0, 'num_vehiculos': 0,
               'clima': 'Sin dato', 'tipo_vehiculo': 'Sin dato'})
      .cache())

print('Registros validos:', df.count())

print('--- Accidentes y victimas por hora del dia ---')
horas = (df.groupBy('hora_dia')
         .agg(F.count('*').alias('accidentes'), F.sum('num_victimas').alias('victimas'))
         .orderBy('hora_dia'))
horas.show(24, False)

print('--- Top 10 zonas (cuadricula de ~1 km) ---')
(df.groupBy('zona')
   .agg(F.count('*').alias('accidentes'), F.sum('num_victimas').alias('victimas'))
   .orderBy(F.desc('accidentes')).show(10, False))

print('--- Condiciones meteorologicas ---')
(df.groupBy('clima')
   .agg(F.count('*').alias('accidentes'),
        F.round(F.avg('num_victimas'), 2).alias('prom_victimas'))
   .orderBy(F.desc('accidentes')).show(truncate=False))

print('--- Tipo de vehiculo ---')
(df.groupBy('tipo_vehiculo')
   .agg(F.count('*').alias('accidentes'),
        F.round(F.avg('num_victimas'), 2).alias('prom_victimas'))
   .orderBy(F.desc('accidentes')).show(10, False))

# Horas criticas = las 5 horas con mas accidentes en el historico
criticas = [r['hora_dia'] for r in
            horas.orderBy(F.desc('accidentes')).limit(5).collect()]
print('Horas criticas:', sorted(criticas))

hist = (horas.select('hora_dia', F.col('accidentes').alias('total_historico'))
        .withColumn('es_critica', F.col('hora_dia').isin(criticas)))
hist.write.mode('overwrite').parquet(SALIDA)
print('Historico guardado en', SALIDA)

spark.stop()
