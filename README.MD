# Tarea-3-Procesamiento-de-datos-con-Apache-Sapark
Datos analizados Covid 19 en Colombia

En este trabajo se analiza cómo se distribuyen los casos de COVID-19 en Colombia según el departamento, el sexo, la edad y el estado de las personas. Como el conjunto de datos tiene más de 6 millones de registros, se uso Apache Spark con Python para procesarlo de forma eficiente.

Este repositorio contiene el código del **procesamiento en batch**, es decir, el que lee los datos completos, los limpia, los analiza y muestra los resultados.

## Conjunto de datos

se analiza el conjunto de datos público "Casos positivos de COVID-19 en Colombia", publicado en datos.gov.co:
https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr

Tiene más de 6,3 millones de registros. Por su tamaño no está incluido en el repositorio y debe descargarse desde la página original. En este caso se almacena en HDFS, el sistema de archivos de Hadoop.

## Qué hace el código

El archivo `batch_covid.py` realiza cuatro etapas:

1. **Carga:** lee el archivo de datos y lo convierte en una tabla de Spark (DataFrame).
2. **Limpieza:** elimina registros repetidos o incompletos, ajusta las edades a años y descarta valores que no tienen sentido.
3. **Análisis exploratorio:** agrupa los casos por departamento, sexo, grupo de edad y estado, y calcula la edad promedio.
4. **Visualización:** muestra las tablas en pantalla y genera gráficas con los resultados.

## Instrucciones de ejecución

De forma general, para ejecutar el proyecto se necesita:

- Tener instalados Python 3, Java y Apache Spark, además de las librerías de `requirements.txt`.
- Descargar el conjunto de datos desde datos.gov.co y guardarlo en el computador o en HDFS.
- Indicar en las primeras líneas de `consumer_covid.py` dónde está el archivo de datos (`RUTA`), en qué carpeta se guardarán las gráficas (`SALIDA`) y cuántos registros se van a procesar (`MUESTRA`).
- Ejecutar el programa con Spark, por ejemplo:

```bash
spark-submit producer_covid.py
```

Las tablas se muestran al terminar y las gráficas quedan en la carpeta indicada.

## Resultados

Se trabajo con una muestra de 200.000 registros (cerca del 3 % del total). Por eso, los resultados describen esa parte de los datos y no representan a todo el país.

- Bogotá y Antioquia concentran cerca del 43 % de los casos de la muestra.
- Los casos se reparten casi por igual entre mujeres y hombres.
- El grupo de edad con más casos es el de 30 a 44 años, y la edad promedio es de 40 años.
- La gran mayoría de los casos fueron leves (96,6 %).
