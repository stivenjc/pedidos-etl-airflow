import sys
import os
sys.path.insert(0, "/home/stivenjc/PycharmProjects/nuevo_projecto")


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.tareas_granulares import tarea_leer, tarea_validar_filas, tarea_cargar_db, tarea_guardar_rechazados

TIENDAS = {
    "norte": "data/pedidos_nort.csv",
    "centro": "data/pedidos_centro.csv",
    "sur": "data/pedidos_sur.csv",
}

with DAG(
        dag_id="pipeline_pedidos_granular",
        start_date=datetime(2026, 8, 1),
        schedule="@daily",
        catchup=False,
        default_args={
            "owner": "stiven",
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        }
) as dag:
    for nombre_tienda, ruta in TIENDAS.items():
        t_leer = PythonOperator(
            task_id=f"leer_{nombre_tienda}",
            python_callable=tarea_leer,
            op_kwargs={"nombre_tienda": nombre_tienda, "ruta": ruta, "fecha": "{{ ds }}"},
        )

        t_validar = PythonOperator(
            task_id=f"validar_filas_{nombre_tienda}",
            python_callable=tarea_validar_filas,
            op_kwargs={"nombre_tienda": nombre_tienda, "fecha": "{{ ds }}"},
        )

        t_cargar = PythonOperator(
            task_id=f"cargar_db_{nombre_tienda}",
            python_callable=tarea_cargar_db,
            op_kwargs={"nombre_tienda": nombre_tienda, "fecha": "{{ ds }}"},
        )

        t_rechazados = PythonOperator(
            task_id=f"guardar_rechazados_{nombre_tienda}",
            python_callable=tarea_guardar_rechazados,
            op_kwargs={"nombre_tienda": nombre_tienda, "fecha": "{{ ds }}"},
        )

        # Cadena de dependencias para ESTA tienda:
        # leer -> validar -> [cargar_db, guardar_rechazados] en paralelo entre sí
        t_leer >> t_validar >> [t_cargar, t_rechazados]

        # Nota: NO conectamos las tiendas entre sí (norte, centro, sur),
        # así que Airflow las corre en paralelo, como ya decidimos antes.
