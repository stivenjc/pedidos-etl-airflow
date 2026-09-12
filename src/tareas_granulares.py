import os
import pandas as pd
from src.utils.logger_config import logger

CARPETA_STAGING = "data/staging"


def tarea_leer(nombre_tienda: str, ruta: str, fecha: str) -> None:
    """
    TAREA 1: Lee el CSV, valida estructura, y guarda el resultado crudo
    en un parquet intermedio para que la siguiente tarea lo retome
    sin tener que volver a leer el CSV original.

    'fecha' identifica la corrida (ej. "2026-09-07") para no pisar
    el resultado de días anteriores -- así se mantiene histórico.
    """
    from src.utils.funtions import leer_y_validar_archivo  # import local para evitar ciclos

    df = leer_y_validar_archivo(ruta, nombre_tienda)

    os.makedirs(CARPETA_STAGING, exist_ok=True)
    ruta_salida = f"{CARPETA_STAGING}/{nombre_tienda}_crudo_{fecha}.parquet"
    df.to_parquet(ruta_salida, index=False)
    logger.info(f"[{nombre_tienda}] Guardado intermedio: {ruta_salida}")


def tarea_validar_filas(nombre_tienda: str, fecha: str) -> None:
    """
    TAREA 2: Lee el parquet crudo de ESTA fecha (no el CSV original),
    separa filas válidas de rechazadas, y guarda AMBAS con la misma fecha.
    Si esta tarea falla y se reintenta, NO vuelve a leer el CSV --
    parte directo del resultado de la tarea 1 de ese mismo día.
    """
    from src.utils.funtions import validar_y_limpiar_filas

    ruta_crudo = f"{CARPETA_STAGING}/{nombre_tienda}_crudo_{fecha}.parquet"
    df = pd.read_parquet(ruta_crudo)

    filas_validas, filas_rechazadas = validar_y_limpiar_filas(df, nombre_tienda)

    filas_validas.to_parquet(f"{CARPETA_STAGING}/{nombre_tienda}_validas_{fecha}.parquet", index=False)
    filas_rechazadas.to_parquet(f"{CARPETA_STAGING}/{nombre_tienda}_rechazadas_{fecha}.parquet", index=False)
    logger.info(f"[{nombre_tienda}] Validación guardada: {len(filas_validas)} válidas, {len(filas_rechazadas)} rechazadas")


def tarea_cargar_db(nombre_tienda: str, fecha: str) -> None:
    """
    TAREA 3: Lee SOLO el parquet de filas válidas de ESTA fecha y las
    carga a Postgres. Si falla aquí (ej. caída de conexión) y se
    reintenta, no repite la lectura del CSV ni la validación -- solo
    vuelve a intentar la carga, usando el mismo parquet ya generado.
    """
    from src.utils.funtions import cargar_pedidos_a_db
    from src.utils.conexion_db import obtener_conexion

    ruta_validas = f"{CARPETA_STAGING}/{nombre_tienda}_validas_{fecha}.parquet"
    filas_validas = pd.read_parquet(ruta_validas)

    engine = obtener_conexion()
    cargar_pedidos_a_db(filas_validas, nombre_tienda, engine)


def tarea_guardar_rechazados(nombre_tienda: str, fecha: str) -> None:
    """
    TAREA 4: Lee SOLO el parquet de filas rechazadas de ESTA fecha y
    las guarda en el CSV acumulativo de cuarentena. Independiente de
    la carga a DB -- por eso puede correr en paralelo con tarea_cargar_db.
    """
    from src.utils.funtions import guardar_filas_rechazadas

    ruta_rechazadas = f"{CARPETA_STAGING}/{nombre_tienda}_rechazadas_{fecha}.parquet"
    filas_rechazadas = pd.read_parquet(ruta_rechazadas)

    guardar_filas_rechazadas(filas_rechazadas, nombre_tienda)