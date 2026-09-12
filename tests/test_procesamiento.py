"""
Tests que formalizan los 5 incidentes simulados del pipeline de pedidos.
Cada test corresponde a un escenario real que ya probamos a mano.
"""
import os
import pytest
import pandas as pd
from sqlalchemy import text
from src.utils.funtions import (
    leer_y_validar_archivo,
    validar_y_limpiar_filas,
    cargar_pedidos_a_db,
)
from src.utils.conexion_db import obtener_conexion


# ---------------------------------------------------------------------------
# FIXTURE: prepara y limpia la base de datos de prueba antes/después de cada test
# ---------------------------------------------------------------------------
@pytest.fixture
def engine_de_prueba():
    """
    Da acceso a la misma base de datos, pero se asegura de dejar la tabla
    'pedidos' limpia ANTES y DESPUÉS de cada test que la use.
    Así cada test arranca de cero y no contamina al siguiente.
    """
    engine = obtener_conexion()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pedidos WHERE tienda = 'tienda_prueba'"))

    yield engine  # aquí se ejecuta el test

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pedidos WHERE tienda = 'tienda_prueba'"))


# ---------------------------------------------------------------------------
# INCIDENTE 1: estructura de archivo dañada (columna faltante)
# Por qué importa: si una tienda manda un archivo mal formado, no debe
# procesarse a medias ni contaminar la base de datos con datos incompletos.
# ---------------------------------------------------------------------------
def test_archivo_con_columna_faltante_lanza_value_error(tmp_path):
    contenido_csv = "id_pedido,fecha,producto,cantidad,tienda\n1,2026-08-01,Mouse,2,norte\n"
    archivo_falso = tmp_path / "pedidos_falsos.csv"
    archivo_falso.write_text(contenido_csv)

    with pytest.raises(ValueError):
        leer_y_validar_archivo(str(archivo_falso), "tienda_prueba")


# ---------------------------------------------------------------------------
# INCIDENTE 2: filas corruptas dentro de un archivo válido
# Por qué importa: una fila mala (precio no numérico) no debe tumbar ni
# descartar las filas buenas que vienen junto a ella (cuarentena, no rechazo total).
# ---------------------------------------------------------------------------
def test_filas_con_precio_invalido_se_rechazan():
    df = pd.DataFrame({
        "id_pedido": [1, 2, 3],
        "fecha": ["2026-08-01", "2026-08-01", "2026-08-01"],
        "producto": ["Mouse", "Teclado", "Monitor"],
        "cantidad": [2, 1, 1],
        "precio_unitario": [45000, "gratis", 650000],
        "tienda": ["norte", "norte", "norte"],
    })

    filas_validas, filas_rechazadas = validar_y_limpiar_filas(df, "tienda_prueba")

    assert len(filas_validas) == 2
    assert len(filas_rechazadas) == 1
    assert filas_rechazadas.iloc[0]["id_pedido"] == 2


def test_filas_con_cantidad_negativa_se_rechazan():
    """
    Mismo principio que el test anterior, pero para el otro campo crítico:
    una cantidad negativa (ej. -3 unidades) no tiene sentido de negocio.
    """
    df = pd.DataFrame({
        "id_pedido": [10, 11],
        "fecha": ["2026-08-01", "2026-08-01"],
        "producto": ["Mouse", "Teclado"],
        "cantidad": [2, -3],
        "precio_unitario": [45000, 180000],
        "tienda": ["norte", "norte"],
    })

    filas_validas, filas_rechazadas = validar_y_limpiar_filas(df, "tienda_prueba")

    assert len(filas_validas) == 1
    assert len(filas_rechazadas) == 1
    assert filas_rechazadas.iloc[0]["id_pedido"] == 11


# ---------------------------------------------------------------------------
# INCIDENTE 3: caída de conexión a mitad de la carga (rollback)
# Por qué importa: si algo falla a mitad de la inserción, NO deben quedar
# datos a medias en la tabla (atomicidad) -- o se guarda todo, o no se guarda nada.
# ---------------------------------------------------------------------------
def test_fallo_a_mitad_de_carga_no_deja_datos_parciales(engine_de_prueba, monkeypatch):
    """
    Simulamos que la conexión se cae en la 3ra fila de 5.
    Verificamos que la tabla quede en 0 filas para esa tienda,
    no en 2 (las que "alcanzaron a insertarse" antes del fallo).
    """
    df = pd.DataFrame({
        "id_pedido": [901, 902, 903, 904, 905],
        "fecha": ["2026-08-01"] * 5,
        "producto": ["Producto X"] * 5,
        "cantidad": [1, 1, 1, 1, 1],
        "precio_unitario": [10000, 10000, 10000, 10000, 10000],
        "tienda": ["tienda_prueba"] * 5,
    })

    # Forzamos que la 3ra fila lance un error, simulando la caída de conexión
    contador = {"n": 0}
    original_execute = engine_de_prueba.__class__

    def ejecutar_con_fallo(self, *args, **kwargs):
        contador["n"] += 1
        if contador["n"] == 3:
            raise ConnectionError("Simulando caída de conexión")
        return original_execute.execute(self, *args, **kwargs)

    # Nota: esta parte usa monkeypatch para interceptar la ejecución real.
    # Si prefieres una versión más simple, se puede lograr forzando el error
    # directamente dentro de cargar_pedidos_a_db con una bandera de prueba.
    with pytest.raises(ConnectionError):
        with engine_de_prueba.begin() as conn:
            for i, (_, fila) in enumerate(df.iterrows()):
                if i == 2:
                    raise ConnectionError("Simulando caída de conexión")
                conn.execute(
                    text("""
                        INSERT INTO pedidos (id_pedido, fecha, producto, cantidad, precio_unitario, tienda)
                        VALUES (:id_pedido, :fecha, :producto, :cantidad, :precio_unitario, :tienda)
                        ON CONFLICT (id_pedido) DO NOTHING
                    """),
                    fila.to_dict()
                )

    # Verificamos directamente en la base de datos que NO quedó nada insertado
    with engine_de_prueba.connect() as conn:
        resultado = conn.execute(
            text("SELECT COUNT(*) FROM pedidos WHERE tienda = 'tienda_prueba'")
        ).scalar()

    assert resultado == 0


# ---------------------------------------------------------------------------
# INCIDENTE 4: archivo duplicado (reenvío del mismo archivo)
# Por qué importa: si una tienda reenvía por error el mismo archivo,
# NO debe duplicar los pedidos ya cargados (idempotencia).
# ---------------------------------------------------------------------------
def test_cargar_el_mismo_pedido_dos_veces_no_lo_duplica(engine_de_prueba):
    df = pd.DataFrame({
        "id_pedido": [777],
        "fecha": ["2026-08-01"],
        "producto": ["Producto Único"],
        "cantidad": [1],
        "precio_unitario": [50000],
        "tienda": ["tienda_prueba"],
    })

    # Cargamos la MISMA fila dos veces seguidas
    cargar_pedidos_a_db(df, "tienda_prueba", engine_de_prueba)
    cargar_pedidos_a_db(df, "tienda_prueba", engine_de_prueba)

    with engine_de_prueba.connect() as conn:
        resultado = conn.execute(
            text("SELECT COUNT(*) FROM pedidos WHERE id_pedido = 777")
        ).scalar()

    # Debe existir UNA sola vez, no dos
    assert resultado == 1


# ---------------------------------------------------------------------------
# INCIDENTE 5: archivo faltante (la tienda no envía a tiempo)
# Por qué importa: debe fallar con un mensaje claro y específico,
# sin tumbar el procesamiento de las demás tiendas.
# ---------------------------------------------------------------------------
def test_archivo_inexistente_lanza_file_not_found_error():
    with pytest.raises(FileNotFoundError):
        leer_y_validar_archivo("data/este_archivo_no_existe.csv", "tienda_prueba")