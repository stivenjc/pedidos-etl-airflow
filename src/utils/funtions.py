import pandas as pd
import os
from sqlalchemy import text
from src.utils.logger_config import logger
RUTA_RECHAZADOS = "logs/pedidos_rechazados.csv"
COLUMNAS_ESPERADAS = {"id_pedido", "fecha", "producto", "cantidad", "precio_unitario", "tienda"}


def guardar_filas_rechazadas(filas_rechazadas: pd.DataFrame, nombre_tienda: str) -> None:
    """
    Guarda las filas rechazadas en un CSV acumulativo, evitando duplicar
    id_pedido que ya hayan sido registrados en corridas anteriores.
    """
    if len(filas_rechazadas) == 0:
        return

    filas_rechazadas = filas_rechazadas.copy()
    filas_rechazadas["fecha_deteccion"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    if os.path.exists(RUTA_RECHAZADOS):
        existentes = pd.read_csv(RUTA_RECHAZADOS)
        ids_ya_registrados = set(existentes["id_pedido"])

        filas_nuevas = filas_rechazadas[~filas_rechazadas["id_pedido"].isin(ids_ya_registrados)]

        if len(filas_nuevas) == 0:
            logger.info(f"[{nombre_tienda}] Todas las filas rechazadas ya estaban registradas, no se agrega nada nuevo")
            return

        filas_nuevas.to_csv(RUTA_RECHAZADOS, mode="a", header=False, index=False)
        logger.info(f"[{nombre_tienda}] {len(filas_nuevas)} fila(s) nueva(s) agregada(s) al registro de rechazados")
    else:
        filas_rechazadas.to_csv(RUTA_RECHAZADOS, mode="w", header=True, index=False)
        logger.info(f"[{nombre_tienda}] Archivo de rechazados creado con {len(filas_rechazadas)} fila(s)")


def validar_y_limpiar_filas(df: pd.DataFrame, nombre_tienda: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Revisa fila por fila que 'precio_unitario' y 'cantidad' sean numéricos y positivos.
    Devuelve (filas_validas, filas_rechazadas).
    """
    df = df.copy()

    # Intentamos convertir a numérico; lo que no se pueda convertir queda como NaN
    df["precio_unitario_num"] = pd.to_numeric(df["precio_unitario"], errors="coerce")
    df["cantidad_num"] = pd.to_numeric(df["cantidad"], errors="coerce")

    es_valida = (
            df["precio_unitario_num"].notna()
            & df["cantidad_num"].notna()
            & (df["precio_unitario_num"] > 0)
            & (df["cantidad_num"] > 0)
    )

    filas_validas = df[es_valida].copy()
    filas_rechazadas = df[~es_valida].copy()

    if len(filas_rechazadas) > 0:
        logger.warning(
            f"[{nombre_tienda}] {len(filas_rechazadas)} fila(s) rechazada(s) por datos inválidos. "
            f"IDs: {filas_rechazadas['id_pedido'].tolist()}"
        )

    filas_validas = filas_validas.drop(columns=["precio_unitario_num", "cantidad_num"])
    filas_rechazadas = filas_rechazadas.drop(columns=["precio_unitario_num", "cantidad_num"])

    logger.info(f"[{nombre_tienda}] {len(filas_validas)} filas válidas de {len(df)} totales")

    return filas_validas, filas_rechazadas


def leer_y_validar_archivo(ruta_archivo: str, nombre_tienda: str) -> pd.DataFrame:
    """
    Lee un archivo CSV de pedidos y valida que tenga las columnas esperadas.
    Lanza un error explícito si la estructura no es la correcta.
    """
    logger.info(f"[{nombre_tienda}] Iniciando lectura de {ruta_archivo}")

    df = pd.read_csv(ruta_archivo)

    columnas_actuales = set(df.columns)

    if columnas_actuales != COLUMNAS_ESPERADAS:
        faltantes = COLUMNAS_ESPERADAS - columnas_actuales
        sobrantes = columnas_actuales - COLUMNAS_ESPERADAS
        logger.error(
            f"[{nombre_tienda}] Estructura inválida. "
            f"Faltantes: {faltantes} | Sobrantes: {sobrantes}"
        )
        raise ValueError(f"Archivo de {nombre_tienda} no tiene la estructura esperada")

    logger.info(f"[{nombre_tienda}] Estructura válida. {len(df)} filas leídas")
    return df


def cargar_pedidos_a_db(df: pd.DataFrame, nombre_tienda: str, engine) -> None:
    """
    Carga los pedidos válidos a la tabla 'pedidos' en Postgres.
    Usa 'ON CONFLICT DO NOTHING' para no duplicar pedidos que ya existan
    (misma idea de idempotencia que usamos con el CSV de rechazados).
    """
    if len(df) == 0:
        logger.info(f"[{nombre_tienda}] No hay filas válidas para cargar")
        return

    filas_insertadas = 0

    with engine.begin() as conn:
        for _, fila in df.iterrows():
            resultado = conn.execute(
                text("""
                    INSERT INTO pedidos (id_pedido, fecha, producto, cantidad, precio_unitario, tienda)
                    VALUES (:id_pedido, :fecha, :producto, :cantidad, :precio_unitario, :tienda)
                    ON CONFLICT (id_pedido) DO NOTHING
                """),
                {
                    "id_pedido": int(fila["id_pedido"]),
                    "fecha": fila["fecha"],
                    "producto": fila["producto"],
                    "cantidad": int(fila["cantidad"]),
                    "precio_unitario": float(fila["precio_unitario"]),
                    "tienda": fila["tienda"],
                }
            )
            filas_insertadas += resultado.rowcount

    logger.info(
        f"[{nombre_tienda}] {filas_insertadas} fila(s) nueva(s) insertada(s) "
        f"({len(df) - filas_insertadas} ya existían)"
    )