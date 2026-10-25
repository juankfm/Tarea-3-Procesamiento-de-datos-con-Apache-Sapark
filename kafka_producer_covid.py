import csv
import json
import time
import subprocess
import unicodedata
import re
from kafka import KafkaProducer

TOPIC = "covid_data"
HDFS_PATH = "/Tarea3Covid/query.csv"   # usa "/Tarea3Covid" si en HDFS es un archivo
HDFS_BIN = "/home/hadoop/hadoop/bin/hdfs"
JAVA_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"
USAR_HDFS = False
LOCAL_PATH = "/tmp/covid_prueba.csv"
PAUSA = 0.5


def normalizar(texto):
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", texto.lower()).strip("_")


def pick(row, *candidatos):
    for c in candidatos:
        if c in row and row[c] not in (None, ""):
            return row[c].strip()
    return None


def a_entero(valor):
    try:
        return int(float(valor))
    except (TypeError, ValueError):
        return None


def abrir_fuente():
    if USAR_HDFS:
        proc = subprocess.Popen(
            ["sudo", "-u", "hadoop", "env", f"JAVA_HOME={JAVA_HOME}",
             HDFS_BIN, "dfs", "-cat", HDFS_PATH],
            stdout=subprocess.PIPE, text=True, encoding="utf-8")
        return proc.stdout
    return open(LOCAL_PATH, encoding="utf-8")
def construir_evento(row):
    return {
        "id_caso": a_entero(pick(row, "id_de_caso", "id_caso")),
        "departamento": pick(row, "nombre_departamento", "departamento_nom", "departamento"),
        "municipio": pick(row, "nombre_municipio", "ciudad_municipio_nom", "municipio"),
        "edad": a_entero(pick(row, "edad")),
        "sexo": pick(row, "sexo"),
        "estado": pick(row, "estado"),
        "recuperado": pick(row, "recuperado"),
        "tipo_contagio": pick(row, "tipo_de_contagio", "fuente_tipo_contagio"),
        "timestamp": int(time.time())
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

fuente = abrir_fuente()
lector = csv.DictReader(fuente)
lector.fieldnames = [normalizar(c) for c in lector.fieldnames]

enviados = 0
for fila in lector:
    evento = construir_evento(fila)
    producer.send(TOPIC, value=evento)
    enviados += 1
    print(f"Sent [{enviados}]: {evento}")
    time.sleep(PAUSA)

producer.flush()
print(f"Total enviados: {enviados}")

