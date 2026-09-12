# Pipeline de Pedidos — Simulación ETL con Airflow

Pipeline de ETL que procesa pedidos diarios de 3 tiendas regionales (Norte, Centro, Sur), diseñado para tolerar y manejar de forma controlada los fallos más comunes de un pipeline en producción: archivos dañados, datos corruptos, caídas de conexión, duplicados y archivos faltantes.

Este proyecto es una simulación construida para practicar y demostrar patrones de manejo de errores, idempotencia y orquestación de nivel producción — no usa datos reales de ninguna empresa.

## Qué resuelve este pipeline

Cada tienda sube diariamente un archivo CSV de pedidos. El pipeline lee, valida, limpia y carga esos pedidos a una base de datos, manejando explícitamente 5 escenarios de fallo:

| # | Incidente | Cómo se maneja |
|---|-----------|-----------------|
| 1 | Archivo con estructura dañada (columna faltante) | Se rechaza solo esa tienda; las demás siguen procesándose con normalidad |
| 2 | Filas corruptas dentro de un archivo válido (precio no numérico, cantidad negativa) | Cuarentena: la fila mala se aísla y se guarda para revisión, sin descartar las filas buenas del mismo archivo |
| 3 | Caída de conexión a mitad de la carga a base de datos | Transacciones con rollback automático: no quedan datos parciales o inconsistentes |
| 4 | Archivo duplicado (reenvío accidental) | Resuelto por diseño: clave primaria + `ON CONFLICT DO NOTHING` evita duplicados sin lógica adicional |
| 5 | Archivo que no llega a tiempo | Error específico y claro (`FileNotFoundError`), sin tumbar el procesamiento de las demás tiendas |

## Arquitectura

```
CSV por tienda → Validar estructura → Validar/limpiar filas → Cargar a Postgres
                                              ↓
                                    Guardar filas rechazadas (cuarentena)
```

Cada paso se guarda como un archivo Parquet intermedio (versionado por fecha de ejecución), de forma que si una tarea falla y se reintenta, no repite el trabajo de los pasos anteriores que ya se completaron con éxito.

Las 3 tiendas se procesan en paralelo entre sí, ya que son completamente independientes (el fallo de una no afecta a las demás).

## Stack técnico

- **Python** — lógica de negocio (lectura, validación, limpieza)
- **pandas** — procesamiento y transformación de datos
- **PostgreSQL** — almacenamiento final de pedidos válidos
- **SQLAlchemy** — conexión y manejo de transacciones con la base de datos
- **Apache Airflow** — orquestación: tareas granulares, paralelismo, reintentos automáticos
- **pytest** — tests automatizados cubriendo los 5 incidentes
- **Parquet** — formato de almacenamiento intermedio entre tareas

## Estructura del proyecto

```
├── data/
│   ├── pedidos_norte.csv
│   ├── pedidos_centro.csv
│   ├── pedidos_sur.csv
│   └── staging/                  # Parquet intermedios, versionados por fecha
├── src/
│   ├── procesamiento.py          # Versión inicial (script suelto, sin orquestador)
│   ├── tareas_granulares.py      # Funciones que usa el DAG de Airflow
│   └── utils/
│       ├── funtions.py           # Lógica de negocio: leer, validar, cargar, cuarentena
│       ├── conexion_db.py        # Conexión a Postgres vía SQLAlchemy
│       └── logger_config.py      # Configuración centralizada de logging
├── airflow_home/
│   └── dags/
│       └── dag_pedidos_granular.py
├── tests/
│   └── test_procesamiento.py     # Tests de los 5 incidentes
├── logs/
│   └── pedidos_rechazados.csv    # Cuarentena de filas rechazadas
├── .env                          # Credenciales de base de datos (no versionado)
└── requirements.txt
```

## Cómo correrlo

### 1. Preparar el entorno

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar la base de datos

Crear la base de datos y la tabla:

```sql
CREATE DATABASE simulacion_pedidos;

CREATE TABLE pedidos (
    id_pedido INTEGER PRIMARY KEY,
    fecha DATE,
    producto TEXT,
    cantidad INTEGER,
    precio_unitario NUMERIC,
    tienda TEXT
);
```

Crear un archivo `.env` en la raíz del proyecto:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=simulacion_pedidos
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
```

### 3. Correr el pipeline de forma manual (sin Airflow)

```bash
python src/procesamiento.py
```

### 4. Correr orquestado con Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```

Abrir `http://localhost:8080`, activar el DAG `pipeline_pedidos_granular` y dispararlo manualmente o dejar que corra en su horario diario.

### 5. Correr los tests

```bash
pytest tests/test_procesamiento.py -v
```

## Decisiones de diseño (por qué se hizo así)

- **Procesar cada tienda por separado, no todo junto**: aísla los fallos. Si una tienda envía un archivo dañado, las demás no se ven afectadas — se sabe exactamente cuál falló y por qué.
- **Cuarentena en vez de rechazo total del archivo**: una fila corrupta no debe costar la pérdida de las demás filas válidas del mismo archivo.
- **Transacciones (`engine.begin()`) para la carga**: garantiza atomicidad — o se inserta todo el lote, o no se inserta nada, nunca un estado a medias.
- **`ON CONFLICT DO NOTHING` + clave primaria**: hace que reenviar el mismo archivo sea seguro por diseño, sin necesitar lógica de deduplicación adicional en el código.
- **Excepciones específicas por tipo** (`FileNotFoundError`, `ValueError`, `ConnectionError`, `Exception` genérico al final): permite mensajes de error claros y accionables, en vez de un log genérico de "algo falló".
- **Archivos intermedios en Parquet entre tareas de Airflow**: permite que un reintento de una tarea específica (ej. la carga a base de datos) no repita el trabajo de tareas anteriores que ya se completaron con éxito.

## Próximos pasos (no implementados aún)

- Containerización con Docker
- Alertas activas (email/Slack) cuando una tarea falla
- Uso de Sensors de Airflow para el caso de archivos que llegan tarde
- CI/CD para correr los tests automáticamente en cada cambio