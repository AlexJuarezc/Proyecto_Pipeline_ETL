from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import time
import requests
import pandas as pd
from urllib.parse import quote
from google.cloud import bigquery
from deep_translator import GoogleTranslator

# Ruta de archivos intermedios
EXTRAER_PATH = "/usr/local/airflow/tmp/datos_clima.csv"
TRANSFORMAR_PATH = "/usr/local/airflow/tmp/datos_clima_transformado.csv"

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10),  
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# Crear el DAG
with DAG(
    'etl_clima_dag',  
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False,  
) as dag:

    # Función de extracción
    @task()
    def extraer():
        API_KEY = "701f7c89b5985b76edf1573401d77a02"
        municipios_df = pd.read_csv('/usr/local/airflow/src/proyecciones_poblacion_filtrada.csv')
        municipios = municipios_df['municipio_nombre'].to_list()
        URL = "https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        datos_clima = []

        for partido in municipios:
            try:
                response = requests.get(URL.format(city=quote(partido), api_key=API_KEY))
                data = response.json()
                if response.status_code == 200:
                    datos_clima.append({
                        "Partido": data["name"],
                        "Temperatura": data["main"]["temp"],
                        "Humedad": str(data["main"]["humidity"]) + "%",
                        "Descripcion": data["weather"][0]["description"]
                    })
            except Exception as e:
                print(f"Error con {partido}: {e}")
            time.sleep(1)
        
        df = pd.DataFrame(datos_clima)
        df.to_csv(EXTRAER_PATH, index=False)
        print("Datos extraídos y guardados.")
    
    # Función de transformación
    @task()
    def transformar():
        df = pd.read_csv(EXTRAER_PATH)
        df["Descripcion"] = df["Descripcion"].apply(lambda x: GoogleTranslator(source='en', target='es').translate(x))
        df.to_csv(TRANSFORMAR_PATH, index=False)
        print("Datos transformados y guardados.")
    
    # Función de carga a BigQuery
    @task()
    def cargar():
        df = pd.read_csv(TRANSFORMAR_PATH)
        client = bigquery.Client()
        dataset_id = 'etl-proyect-449815.clima_ciudades_de_buenos_aires'
        table_id = 'Clima_ciudades'
        table_ref = f"{dataset_id}.{table_id}"

        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)  
            print(f"La tabla {table_id} ya existe.")
        except:
            schema = [
                bigquery.SchemaField("Partido", "STRING"),
                bigquery.SchemaField("Temperatura", "FLOAT"),
                bigquery.SchemaField("Humedad", "STRING"),
                bigquery.SchemaField("Descripcion", "STRING"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Tabla {table_id} creada en BigQuery.")

        # Cargar los datos en BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("Datos cargados en BigQuery.")

    # Definir las dependencias entre las tareas
    extraer() >> transformar() >> cargar()
