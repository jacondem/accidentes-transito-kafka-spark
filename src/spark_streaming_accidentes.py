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


def consola(df, nombre, salida):
    return (df.writeStream.queryName(nombre).outputMode(salida)
            .format('console')
            .option('truncate', 'false').option('numRows', 20)
            .trigger(processingTime='10 seconds').start())


consultas = []

if modo in ('ventana', 'todas'):
    ventana = (reportes.withWatermark('event_time', '2 minutes')
        .groupBy(F.window('event_time', '1 minute'), 'clima')
        .agg(F.count('*').alias('accidentes'),
             F.sum('num_victimas').alias('victimas'),
             F.round(F.avg('num_vehiculos'), 2).alias('prom_vehiculos')))
    consultas.append(consola(ventana, 'ventana_clima', 'update'))

if modo in ('horario', 'todas'):
    horario = (reportes.join(hist, 'hora_dia', 'left')
        .groupBy('hora_dia', 'es_critica')
        .agg(F.count('*').alias('accidentes_en_vivo'),
             F.sum('num_victimas').alias('victimas'))
        .orderBy(F.desc('accidentes_en_vivo')))
    consultas.append(consola(horario, 'horario_critico', 'complete'))

if modo in ('zonas', 'todas'):
    zonas = (reportes.groupBy('zona')
        .agg(F.count('*').alias('accidentes'),
             F.sum('num_victimas').alias('victimas'))
        .orderBy(F.desc('accidentes')).limit(10))
    consultas.append(consola(zonas, 'zonas_criticas', 'complete'))

if modo in ('alertas', 'todas'):
    alertas = (reportes
        .filter((F.col('num_victimas') >= 3) | (F.col('num_vehiculos') >= 4))
        .select('event_time', 'hora', 'zona', 'num_victimas',
                'num_vehiculos', 'tipo_vehiculo', 'clima'))
    consultas.append(consola(alertas, 'alertas_graves', 'append'))

if not consultas:
    raise SystemExit('Modo no valido: use ventana, horario, zonas, alertas o todas')

spark.streams.awaitAnyTermination()
