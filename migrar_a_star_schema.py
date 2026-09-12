"""
Migra los datos de la tabla plana `pedidos` al star schema
(dim_producto, dim_tienda, dim_fecha, hechos_pedidos).

Es un script de una sola corrida sobre datos historicos, no una tarea
de Airflow. Se corre local, una vez, despues de crear las tablas nuevas
con crear_tablas_star_schema.sql.
"""

from sqlalchemy import text
from src.utils.conexion_db import obtener_conexion  # reusa tu conexion existente

engine = obtener_conexion()


def obtener_o_crear_id(conn, tabla, columna_id, columna_valor, valor):
    """
    Busca el id de una dimension por su valor (ej. nombre_producto).
    Si no existe, lo crea. Devuelve el id en ambos casos.
    """
    fila = conn.execute(
        text(f"SELECT {columna_id} FROM {tabla} WHERE {columna_valor} = :valor"),
        {"valor": valor},
    ).fetchone()

    if fila:
        return fila[0]

    fila_nueva = conn.execute(
        text(
            f"INSERT INTO {tabla} ({columna_valor}) VALUES (:valor) "
            f"RETURNING {columna_id}"
        ),
        {"valor": valor},
    ).fetchone()
    return fila_nueva[0]


def obtener_o_crear_fecha(conn, fecha):
    """
    Igual que obtener_o_crear_id, pero para dim_fecha: si la fecha es
    nueva, calcula los atributos derivados (dia de semana, mes, etc.)
    antes de insertarla.
    """
    fila = conn.execute(
        text("SELECT id_fecha FROM dim_fecha WHERE fecha = :fecha"),
        {"fecha": fecha},
    ).fetchone()

    if fila:
        return fila[0]

    dia_semana = fecha.strftime("%A")
    mes = fecha.month
    trimestre = (mes - 1) // 3 + 1
    anio = fecha.year
    es_fin_de_semana = fecha.weekday() >= 5

    fila_nueva = conn.execute(
        text(
            """
            INSERT INTO dim_fecha
                (fecha, dia_semana, mes, trimestre, anio, es_fin_de_semana)
            VALUES (:fecha, :dia_semana, :mes, :trimestre, :anio, :es_fin_de_semana) RETURNING id_fecha
            """
        ),
        {
            "fecha": fecha,
            "dia_semana": dia_semana,
            "mes": mes,
            "trimestre": trimestre,
            "anio": anio,
            "es_fin_de_semana": es_fin_de_semana,
        },
    ).fetchone()
    return fila_nueva[0]


def migrar_a_star_schema():
    with engine.begin() as conn:
        pedidos = conn.execute(text("SELECT * FROM pedidos")).fetchall()

        for pedido in pedidos:
            id_producto = obtener_o_crear_id(
                conn, "dim_producto", "id_producto", "nombre_producto", pedido.producto
            )
            id_tienda = obtener_o_crear_id(
                conn, "dim_tienda", "id_tienda", "nombre_tienda", pedido.tienda
            )
            id_fecha = obtener_o_crear_fecha(conn, pedido.fecha)

            conn.execute(
                text(
                    """
                    INSERT INTO hechos_pedidos
                    (id_pedido, id_producto, id_tienda, id_fecha, cantidad, precio_unitario)
                    VALUES (:id_pedido, :id_producto, :id_tienda, :id_fecha, :cantidad,
                            :precio_unitario) ON CONFLICT (id_pedido) DO NOTHING
                    """
                ),
                {
                    "id_pedido": pedido.id_pedido,
                    "id_producto": id_producto,
                    "id_tienda": id_tienda,
                    "id_fecha": id_fecha,
                    "cantidad": pedido.cantidad,
                    "precio_unitario": pedido.precio_unitario,
                },
            )

    print("Migracion completada.")


if __name__ == "__main__":
    migrar_a_star_schema()
