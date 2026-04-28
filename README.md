# ChileCompra Data Processor v2.0

**Herramienta automatizada para descarga y procesamiento masivo de datos de licitaciones publicas de Chile (2015-2026).**

Extrae, transforma y consolida mas de 30 GB de datos crudos del portal de datos abiertos de ChileCompra en archivos Parquet compactos y listos para analisis, todo ejecutable con un solo clic en Windows.

### Caracteristicas principales

- **Guardado incremental:** Cada mes procesado se guarda inmediatamente en disco. Si el proceso se interrumpe, no se pierde nada.
- **Reanudacion automatica:** Al reiniciar, detecta los meses ya procesados y continua desde donde quedo.
- **Interrupcion limpia (Ctrl+C):** Guarda el progreso actual y sale de forma segura.
- **Instalacion automatica de Python:** En Windows, si Python no esta instalado, ofrece descargarlo e instalarlo automaticamente.
- **Progreso en tiempo real:** Muestra porcentaje completado, tiempo estimado restante (ETA) y registros procesados.

---

## El Problema

El portal de [Datos Abiertos de ChileCompra](https://datos-abiertos.chilecompra.cl/) publica mensualmente archivos CSV con informacion detallada de todas las licitaciones publicas del Estado de Chile. Sin embargo, trabajar con estos datos presenta desafios significativos:

- **Volumen masivo:** Cada archivo mensual pesa entre 300-700 MB descomprimido. El total historico supera los 30 GB.
- **136 archivos** distribuidos en ZIPs mensuales desde 2015 hasta 2026.
- **Formato inconsistente:** Encoding latin-1, separador punto y coma, campos con comillas.
- **Memoria insuficiente:** Cargar todos los datos en RAM es inviable en la mayoria de equipos.

## La Solucion

Este procesador resuelve todos estos problemas con una estrategia de **streaming por chunks con persistencia incremental**:

1. **Descarga secuencial:** Obtiene cada ZIP mensual desde Azure Blob Storage.
2. **Extraccion y procesamiento:** Descomprime el CSV y lo lee en chunks de 100,000 filas.
3. **Guardado inmediato:** Despues de procesar cada mes, guarda los resultados en Parquet al instante.
4. **Eliminacion del crudo:** Borra cada CSV crudo despues de procesarlo para ahorrar disco.
5. **Registro de progreso:** Mantiene un archivo `_progress.json` con los meses completados.
6. **Consolidacion final:** Al terminar todos los meses, genera metricas derivadas e indice de oportunidad.

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

## Controles de Ejecucion

| Comando / Accion | Descripcion |
|------------------|-------------|
| `python chilecompra_processor.py` | Ejecutar (o reanudar si hay progreso previo) |
| `python chilecompra_processor.py --reset` | Reiniciar desde cero (borra datos procesados y progreso) |
| `Ctrl+C` durante la ejecucion | Guardar progreso actual y salir limpiamente |
| Ejecutar de nuevo despues de Ctrl+C | Reanuda automaticamente desde el ultimo mes completado |

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
