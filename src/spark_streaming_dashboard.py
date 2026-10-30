import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, LongType)

KAFKA = 'localhost:9092'
TOPICO = 'accidentes_reportes'
HIST = '/home/vboxuser/resultados/hist_hora'
modo = sys.argv[1] if len(sys.argv) > 1 else 'todas'

spark = (SparkSession.builder.appName('AccidentesTiempoReal')
         .config('spark.sql.shuffle.partitions', '4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

esquema = StructType([
    StructField('fecha', StringType()),
    StructField('hora', StringType()),
    StructField('latitud', DoubleType()),
    StructField('longitud', DoubleType()),
    StructField('num_victimas', IntegerType()),
    StructField('num_vehiculos', IntegerType()),
    StructField('tipo_vehiculo', StringType()),
    StructField('clima', StringType()),
    StructField('event_time', LongType()),
])

crudo = (spark.readStream.format('kafka')
         .option('kafka.bootstrap.servers', KAFKA)
         .option('subscribe', TOPICO)
         .option('startingOffsets', 'latest')
         .load())

reportes = (crudo
    .select(F.from_json(F.col('value').cast('string'), esquema).alias('d'))
    .select('d.*')
    .withColumn('event_time', F.col('event_time').cast('timestamp'))
    .withColumn('hora_dia', F.split(F.col('hora'), ':').getItem(0).cast('int'))
    .withColumn('zona', F.concat_ws(',', F.round('latitud', 2).cast('string'),
                                   F.round('longitud', 2).cast('string')))
    .fillna({'num_victimas': 0, 'num_vehiculos': 0,
             'clima': 'Sin dato', 'tipo_vehiculo': 'Sin dato'}))

# Resultado del analisis historico (tabla estatica)
hist = spark.read.parquet(HIST)

import sqlite3

DB = '/home/vboxuser/resultados/dashboard.db'


def a_sqlite(tabla, modo):
    def escribir(batch_df, batch_id):
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        con = sqlite3.connect(DB, timeout=30)
        pdf.to_sql(tabla, con, if_exists=modo, index=False)
        con.close()
    return escribir


def lanzar(df, nombre, salida, tabla, modo):
    return (df.writeStream.queryName(nombre).outputMode(salida)
            .foreachBatch(a_sqlite(tabla, modo))
            .trigger(processingTime='10 seconds').start())


horario = (reportes.join(hist, 'hora_dia', 'left')
    .groupBy('hora_dia', F.coalesce('es_critica', F.lit(False)).alias('es_critica'))
    .agg(F.count('*').alias('accidentes_en_vivo'),
         F.sum('num_victimas').alias('victimas')))

zonas = (reportes.groupBy('zona')
    .agg(F.count('*').alias('accidentes'),
         F.sum('num_victimas').alias('victimas'),
         F.avg('latitud').alias('lat'),
         F.avg('longitud').alias('lon'))
    .orderBy(F.desc('accidentes')).limit(50))

clima = (reportes.groupBy('clima')
    .agg(F.count('*').alias('accidentes'),
         F.sum('num_victimas').alias('victimas')))

ritmo = (reportes.groupBy(F.window('event_time', '1 minute').alias('w'))
    .agg(F.count('*').alias('accidentes'),
         F.sum('num_victimas').alias('victimas'))
    .select(F.col('w.start').alias('minuto'), 'accidentes', 'victimas'))

alertas = (reportes
    .filter((F.col('num_victimas') >= 3) | (F.col('num_vehiculos') >= 4))
    .select('event_time', 'hora', 'zona', 'num_victimas',
            'num_vehiculos', 'tipo_vehiculo', 'clima'))

lanzar(horario, 'db_horario', 'complete', 'horario', 'replace')
lanzar(zonas, 'db_zonas', 'complete', 'zonas', 'replace')
lanzar(clima, 'db_clima', 'complete', 'clima', 'replace')
lanzar(ritmo, 'db_ritmo', 'complete', 'ritmo', 'replace')
lanzar(alertas, 'db_alertas', 'append', 'alertas', 'append')

spark.streams.awaitAnyTermination()
