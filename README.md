# Análisis de accidentes de tránsito en tiempo real con Kafka y Spark

Proyecto del curso Big Data (UNAD). Procesa el histórico de accidentes con
Spark y simula la llegada de nuevos reportes con Apache Kafka para detectar
horarios críticos, zonas de alta incidencia y condiciones asociadas a lesiones.

## Arquitectura
Productor (Python) -> Kafka (KRaft) -> Spark Structured Streaming -> SQLite -> Streamlit

## Requisitos
- Ubuntu 22.04, Java 11, Spark 3.5.3 (Scala 2.12), Python 3.10
- Apache Kafka 3.9.1
- `pip install -r requirements.txt`

## Uso
1. Descargar el CSV (ver `data/README.md`).
2. Instalar e iniciar Kafka y crear el tema `accidentes_reportes`.
3. `spark-submit src/spark_batch_historico.py`
4. `python3 src/kafka_producer_accidentes.py`
5. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_accidentes.py`
6. (Opcional) Dashboard: ver `src/spark_streaming_dashboard.py` y `src/dashboard.py`. jonatan conde - Ingenieria de Sistemas EOF
cat > README.md << 'EOF'
# Análisis de accidentes de tránsito en tiempo real con Kafka y Spark

Proyecto del curso Big Data (UNAD). Procesa el histórico de accidentes con
Spark y simula la llegada de nuevos reportes con Apache Kafka para detectar
horarios críticos, zonas de alta incidencia y condiciones asociadas a lesiones.

## Arquitectura
Productor (Python) -> Kafka (KRaft) -> Spark Structured Streaming -> SQLite -> Streamlit

## Requisitos
- Ubuntu 22.04, Java 11, Spark 3.5.3 (Scala 2.12), Python 3.10
- Apache Kafka 3.9.1
- `pip install -r requirements.txt`

## Uso
1. Descargar el CSV (ver `data/README.md`).
2. Instalar e iniciar Kafka y crear el tema `accidentes_reportes`.
3. `spark-submit src/spark_batch_historico.py`
4. `python3 src/kafka_producer_accidentes.py`
5. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_streaming_accidentes.py`
6. (Opcional) Dashboard: ver `src/spark_streaming_dashboard.py` y `src/dashboard.py`. jonatan conde - Ingenieria de Sistemas EOF


