from src.utils.conexion_db import obtener_conexion
from src.utils.funtions import validar_y_limpiar_filas, guardar_filas_rechazadas, leer_y_validar_archivo, \
    cargar_pedidos_a_db
from src.utils.logger_config import logger

engine = obtener_conexion()


def procesar_todas_las_tiendas(rutas_por_tienda: dict) -> dict:
    """
    rutas_por_tienda: {"norte": "data/pedidos_norte.csv", "centro": "...", "sur": "..."}
    Devuelve un dict con los DataFrames de las tiendas que sí se procesaron bien.
    Si TODAS las tiendas fallan, detiene el pipeline con una excepción.
    """
    resultados = {}
    tiendas_fallidas = []

    for nombre_tienda, ruta in rutas_por_tienda.items():
        try:
            df = leer_y_validar_archivo(ruta, nombre_tienda)

            df_filas_validas, df_filas_rechazadas = validar_y_limpiar_filas(df, nombre_tienda)

            cargar_pedidos_a_db(df, nombre_tienda, engine)
            guardar_filas_rechazadas(df_filas_rechazadas, nombre_tienda)

            resultados[nombre_tienda] = df_filas_validas

        except FileNotFoundError as e:
            logger.error(f"[{nombre_tienda}] Archivo no encontrado en la ruta esperada: {e}")
            tiendas_fallidas.append(nombre_tienda)
            continue

        except ValueError as e:
            logger.error(f"[{nombre_tienda}] Se omite esta tienda por error: {e}")
            tiendas_fallidas.append(nombre_tienda)
            continue

        except ConnectionError as e:
            logger.error(f"[{nombre_tienda}] Error de conexión a la base de datos: {e}")
            tiendas_fallidas.append(nombre_tienda)
            continue

        except Exception as e:
            logger.error(f"[{nombre_tienda}] Error inesperado ({type(e).__name__}): {e}")
            tiendas_fallidas.append(nombre_tienda)
            continue

    if len(tiendas_fallidas) == len(rutas_por_tienda):
        logger.critical("Todas las tiendas fallaron. Deteniendo el pipeline.")
        raise RuntimeError("Fallo total: ninguna tienda se pudo procesar")

    if tiendas_fallidas:
        logger.warning(f"Tiendas con fallo en esta corrida: {tiendas_fallidas}")

    return resultados


if __name__ == "__main__":

    rutas = {
        "norte": "data/pedidos_norte.csv",
        "centro": "data/pedidos_centro.csv",
        "sur": "data/pedidos_sur.csv",
    }

    resultado = procesar_todas_las_tiendas(rutas)
    print(f"\nTiendas procesadas con éxito: {list(resultado.keys())}")
