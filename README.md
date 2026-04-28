# ChileCompra Data Processor v3.0

**Herramienta automatizada para descarga y procesamiento masivo de datos de licitaciones publicas de Chile (2015-2026).**

Extrae, transforma y consolida mas de 30 GB de datos crudos del portal de datos abiertos de ChileCompra en archivos Parquet compactos y listos para analisis. Puede ejecutarse en GitHub Actions para publicar los resultados como releases, o localmente con un solo clic en Windows.

### Caracteristicas principales

- **Ejecucion automatizada en GitHub Actions:** Genera y publica los Parquet como releases del repositorio.
- **Agregacion mensual correcta:** Promedios, conteos unicos y medianas combinables entre chunks.
- **Guardado mensual particionado:** Cada mes guarda sus agregados en `data/processed/monthly/<tag>/` y al final se consolidan en los 7 archivos definitivos.
- **Reanudacion automatica:** Si el proceso se interrumpe, al reiniciar detecta los meses procesados y continua desde donde quedo.
- **Periodo configurable:** Procesa todo el historico o un rango personalizado de años.
- **Interrupcion limpia (Ctrl+C):** Guarda el progreso actual y sale de forma segura.
- **Instalacion automatica de Python en Windows:** Si Python no esta instalado, ofrece descargarlo e instalarlo automaticamente.

---

## El Problema

El portal de [Datos Abiertos de ChileCompra](https://datos-abiertos.chilecompra.cl/) publica mensualmente archivos CSV con informacion detallada de todas las licitaciones publicas del Estado de Chile. Sin embargo, trabajar con estos datos presenta desafios significativos:

- **Volumen masivo:** Cada archivo mensual pesa entre 300-700 MB descomprimido. El total historico supera los 30 GB.
- **136 archivos** distribuidos en ZIPs mensuales desde 2015 hasta 2026.
- **Formato inconsistente:** Encoding latin-1, separador punto y coma, campos con comillas.
- **Memoria insuficiente:** Cargar todos los datos en RAM es inviable en la mayoria de equipos.

## La Solucion

Este procesador resuelve todos estos problemas con una estrategia de **streaming por chunks con guardado mensual particionado**:

1. **Descarga secuencial:** Obtiene cada ZIP mensual desde Azure Blob Storage.
2. **Extraccion y procesamiento:** Descomprime el CSV y lo lee en chunks de 100,000 filas.
3. **Agregacion combinable:** Mantiene `sum + count` para promedios y `set()` para conteos unicos, asegurando resultados correctos al combinar chunks.
4. **Guardado por mes:** Cada mes escribe sus 5 agregados en `data/processed/monthly/<tag>/` (~200 KB por mes).
5. **Eliminacion del crudo:** Borra cada CSV crudo despues de procesarlo para ahorrar disco.
6. **Registro de progreso:** Mantiene un archivo `_progress.json` con los meses completados.
7. **Consolidacion final:** Al terminar, lee todos los archivos mensuales y genera los 7 archivos definitivos con metricas anuales e indice de oportunidad.

El resultado: **7 archivos Parquet** que pesan menos de 50 MB en total, conteniendo toda la inteligencia extraida de mas de 30 GB de datos crudos.

---

## Como obtener los datos procesados

### Opcion 1: Descargar desde Releases (mas rapido)

Si solo necesitas los archivos Parquet para usarlos en tu analisis, descargalos directamente desde la [pagina de Releases](../../releases) del repositorio. Cada release contiene los 7 archivos generados en una corrida automatizada.

```bash
# Ejemplo: descargar el release mas reciente con gh CLI
gh release download --repo djwillichile/chilecompra-data-processor --pattern '*.parquet' --pattern '*.json'
```

### Opcion 2: Ejecutar el workflow en GitHub Actions

Si tienes un fork del repositorio y quieres regenerar los Parquet:

1. Entra a la pestaña **Actions** del repositorio.
2. Selecciona el workflow **"Procesar datos ChileCompra"**.
3. Click en **"Run workflow"**.
4. Opcionalmente ajusta:
   - `reset`: `true` para ignorar progreso anterior.
   - `start_year` / `end_year`: rango de años a procesar (default: 2015 - 2026).
5. Al terminar (~30-60 min), los archivos quedan publicados como un nuevo release con tag `data-YYYY-MM-DD`.

### Opcion 3: Ejecucion local en Windows

#### Doble clic (recomendado para usuarios sin experiencia)

1. Descarga este repositorio (o clona con `git clone`).
2. Haz **doble clic en `EJECUTAR.bat`**.
3. El script automaticamente:
   - Detecta o instala Python si es necesario.
   - Instala las dependencias (`pandas`, `pyarrow`, `requests`).
   - Pregunta si quieres todo el historico o un periodo personalizado.
   - Descarga y procesa los archivos de ChileCompra.
   - Genera los Parquet en `data/processed/`.

#### Linea de comandos

```bash
pip install -r requirements.txt

# Procesar todo el historico
python chilecompra_processor.py

# Procesar un periodo especifico
python chilecompra_processor.py --start-year 2020 --end-year 2024

# Reiniciar desde cero
python chilecompra_processor.py --reset
```

### Opcion 4: Linux / macOS

```bash
pip3 install -r requirements.txt
python3 chilecompra_processor.py
```

---

## Controles de Ejecucion

| Comando / Accion | Descripcion |
|------------------|-------------|
| `python chilecompra_processor.py` | Ejecutar (o reanudar si hay progreso previo) |
| `python chilecompra_processor.py --reset` | Reiniciar desde cero (borra datos procesados y progreso) |
| `python chilecompra_processor.py --start-year 2020 --end-year 2024` | Procesar solo un rango de años |
| `python chilecompra_processor.py --help` | Ver todas las opciones disponibles |
| `Ctrl+C` durante la ejecucion | Guardar progreso actual y salir limpiamente |

### Ejemplo de reanudacion

```
  [REANUDANDO] 45/136 meses ya procesados.
  [PENDIENTE]  91 meses por procesar.
  [REGISTROS]  3,245,678 registros acumulados.

  [46/136] (33%) ETA: 0:42:15 | 2018-10 descargando...(28.5MB) procesando (456MB)...OK (87,432 registros) [GUARDADO]
```

---

## Archivos Generados

| Archivo | Descripcion | Uso principal |
|---------|-------------|---------------|
| `agregado_mensual_categoria.parquet` | Gasto mensual por rubro/categoria | Series de tiempo, estacionalidad |
| `agregado_anual_organismo_clean.parquet` | Gasto anual por organismo y sector | Analisis institucional |
| `agregado_anual_region_clean.parquet` | Gasto anual por region | Analisis territorial |
| `agregado_anual_proveedor_top500.parquet` | Top 500 proveedores por anio | Concentracion de mercado |
| `competencia_por_rubro.parquet` | Nivel de competencia por rubro | Oportunidades de mercado |
| `metricas_anuales_categoria.parquet` | Metricas con indice de oportunidad | Inteligencia estrategica |
| `sankey_tops_2024.json` | Top 15 por nivel (año mas reciente) | Visualizacion de flujos |

Adicionalmente, durante la ejecucion se genera un directorio `data/processed/monthly/<año-mes>/` con los agregados intermedios de cada mes (5 parquets pequeños). Sirve como cache para reanudar y se puede borrar al finalizar.

---

## Estructura de Datos

### agregado_mensual_categoria.parquet

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| anio | int | Anio (2015-2026) |
| mes | int | Mes (1-12) |
| Rubro1 | str | Categoria principal de la licitacion |
| n_licitaciones | int | Cantidad de licitaciones |
| monto_adjudicado | float | Monto total en CLP |
| oferentes_promedio | float | Promedio de oferentes por licitacion |
| n_proveedores | int | Proveedores unicos en el mes |
| n_organismos | int | Organismos compradores unicos en el mes |

### metricas_anuales_categoria.parquet

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| anio | int | Anio |
| Rubro1 | str | Categoria |
| monto_adjudicado | float | Monto total anual en CLP |
| n_licitaciones | int | Cantidad de licitaciones |
| oferentes_promedio | float | Competencia promedio |
| crecimiento_monto_pct | float | Crecimiento interanual del monto (%) |
| crecimiento_lic_pct | float | Crecimiento interanual de licitaciones (%) |
| indice_oportunidad | float | Indice compuesto (0-1): alto monto + alto crecimiento + baja competencia |

---

## Requisitos del Sistema

- **Python 3.9+** (el .bat lo instala automaticamente si no existe)
- **Conexion a internet** (para descargar los ZIPs de ChileCompra)
- **2 GB de RAM** disponibles (procesamiento por chunks)
- **2 GB de disco** temporales (se liberan al finalizar)
- **50 MB** para los archivos Parquet finales

## Tiempo Estimado

| Conexion | Tiempo aproximado |
|----------|-------------------|
| GitHub Actions | 25-40 minutos |
| 100 Mbps | 20-30 minutos |
| 50 Mbps | 30-45 minutos |
| 20 Mbps | 45-75 minutos |

---

## Fuente de Datos

Los datos provienen del portal oficial de [Datos Abiertos de ChileCompra](https://datos-abiertos.chilecompra.cl/), administrado por la Direccion ChileCompra del Ministerio de Hacienda de Chile.

**URL base de descarga:**
```
https://transparenciachc.blob.core.windows.net/lic-da/{anio}-{mes}.zip
```

Los datos son publicos y de libre acceso bajo la politica de datos abiertos del Estado de Chile.

---

## Casos de Uso

- **Analisis de mercado:** Identificar sectores con alto gasto y baja competencia.
- **Inteligencia de negocios:** Detectar oportunidades para proveedores del Estado.
- **Investigacion academica:** Estudiar patrones de gasto publico.
- **Periodismo de datos:** Transparencia y rendicion de cuentas.
- **Consultoria:** Asesorar empresas que quieren participar en licitaciones.

---

## Proyecto Relacionado

Este procesador genera los datos utilizados en el proyecto de analisis completo:
[chile-public-procurement-analysis](https://github.com/djwillichile/chile-public-procurement-analysis)

---

## Autor

**Guillermo Fuentes** ([@djwillichile](https://github.com/djwillichile))
Cientifico de datos geoespaciales | Consultor ambiental | Docente universitario

---

## Licencia

MIT License - Libre para uso comercial y academico.
