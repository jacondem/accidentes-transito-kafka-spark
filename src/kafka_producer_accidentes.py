import argparse
import csv
import json
import random
import time
from kafka import KafkaProducer

COLUMNAS = {
    'fecha': 'Accident Date',
    'hora': 'Time',
    'latitud': 'Latitude',
    'longitud': 'Longitude',
    'num_victimas': 'Number_of_Casualties',
    'num_vehiculos': 'Number_of_Vehicles',
    'tipo_vehiculo': 'Vehicle_Type',
    'clima': 'Weather_Conditions',
}


def a_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def a_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def construir_reporte(fila):
    r = {
        'fecha': fila.get(COLUMNAS['fecha']),
        'hora': fila.get(COLUMNAS['hora']),
        'latitud': a_float(fila.get(COLUMNAS['latitud'])),
        'longitud': a_float(fila.get(COLUMNAS['longitud'])),
        'num_victimas': a_int(fila.get(COLUMNAS['num_victimas'])),
        'num_vehiculos': a_int(fila.get(COLUMNAS['num_vehiculos'])),
        'tipo_vehiculo': fila.get(COLUMNAS['tipo_vehiculo']),
        'clima': fila.get(COLUMNAS['clima']),
    }
    if r['latitud'] is None or r['longitud'] is None or not r['hora']:
        return None
    r['event_time'] = int(time.time())
    return r


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--archivo', default='/home/vboxuser/data/Road_Accident_Data.csv')
    p.add_argument('--servidor', default='localhost:9092')
    p.add_argument('--topico', default='accidentes_reportes')
    p.add_argument('--delay', type=float, default=0.5,
                   help='segundos entre reportes')
    a = p.parse_args()

    with open(a.archivo, newline='', encoding='utf-8-sig') as f:
        filas = list(csv.DictReader(f))
    faltan = [c for c in COLUMNAS.values() if c not in filas[0]]
    if faltan:
        raise SystemExit('Columnas no encontradas en el CSV: %s' % faltan)

    random.shuffle(filas)
    producer = KafkaProducer(
        bootstrap_servers=[a.servidor],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    enviados = 0
    try:
        for fila in filas:
            reporte = construir_reporte(fila)
            if reporte is None:
                continue
            producer.send(a.topico, value=reporte)
            enviados += 1
            print('Enviado #%d: %s' % (enviados, reporte))
            time.sleep(a.delay)
    except KeyboardInterrupt:
        print('Detenido por el usuario.')
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    main()
