# ChileCompra Data Processor

**Herramienta automatizada para descarga y procesamiento masivo de datos de licitaciones publicas de Chile (2015-2026).**

Extrae, transforma y consolida mas de 30 GB de datos crudos del portal de datos abiertos de ChileCompra en archivos Parquet compactos y listos para analisis, todo ejecutable con un solo clic en Windows.

---

## El Problema

El portal de [Datos Abiertos de ChileCompra](https://datos-abiertos.chilecompra.cl/) publica mensualmente archivos CSV con informacion detallada de todas las licitaciones publicas del Estado de Chile. Sin embargo, trabajar con estos datos presenta desafios significativos:

- **Volumen masivo:** Cada archivo mensual pesa entre 300-700 MB descomprimido. El total historico supera los 30 GB.
- **136 archivos** distribuidos en ZIPs mensuales desde 2015 hasta 2026.
- **Formato inconsistente:** Encoding latin-1, separador punto y coma, campos con comillas.
- **Memoria insuficiente:** Cargar todos los datos en RAM es inviable en la mayoria de equipos.

## La Solucion

Este procesador resuelve todos estos problemas con una estrategia de **streaming por chunks**:

1. **Descarga incremental:** Obtiene cada ZIP mensual desde Azure Blob Storage.
2. **Extraccion en memoria:** Descomprime el CSV sin almacenar archivos intermedios innecesarios.
3. **Procesamiento por chunks:** Lee 100,000 filas a la vez, agrega y libera memoria.
4. **Eliminacion inmediata:** Borra cada CSV crudo despues de procesarlo.
5. **Consolidacion final:** Genera archivos Parquet compactos con las agregaciones necesarias.

El resultado: **7 archivos Parquet** que pesan menos de 50 MB en total, conteniendo toda la inteligencia extraida de mas de 30 GB de datos crudos.

---

## Uso Rapido (Windows)

### Opcion 1: Doble clic (recomendado)

1. Descarga este repositorio (o clona con `git clone`).
2. Haz **doble clic en `EJECUTAR.bat`**.
3. El script automaticamente:
   - Detecta o instala Python si es necesario.
   - Instala las dependencias (`pandas`, `pyarrow`, `requests`).
   - Descarga y procesa los 136 archivos de ChileCompra.
   - Genera los Parquet en `data/processed/`.

### Opcion 2: Linea de comandos

```bash
pip install pandas pyarrow requests
python chilecompra_processor.py
```

### Opcion 3: Linux / macOS

```bash
pip3 install pandas pyarrow requests
python3 chilecompra_processor.py
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
| `sankey_tops_2024.json` | Top 15 por nivel (2024) | Visualizacion de flujos |

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
| n_proveedores | int | Proveedores unicos |
| n_organismos | int | Organismos compradores unicos |

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
