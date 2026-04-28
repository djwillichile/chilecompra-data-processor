# Ejemplos para el repositorio consumidor

Esta carpeta contiene archivos de ejemplo pensados para copiarse al repositorio
[chile-public-procurement-analysis](https://github.com/djwillichile/chile-public-procurement-analysis)
(o cualquier otro repo que quiera consumir los Parquet generados).

La estructura de directorios refleja exactamente como deben quedar en el repo
destino.

## Contenido

| Archivo | Destino | Proposito |
|---------|---------|-----------|
| `scripts/download_data.sh` | `scripts/download_data.sh` del repo de analisis | Descarga manual de los Parquet del ultimo release |
| `.github/workflows/sync-data.yml` | `.github/workflows/sync-data.yml` del repo de analisis | Sincronizacion automatica mensual + commit |

## Como usarlos

1. **Copiar los archivos** al repositorio de analisis manteniendo la misma ruta relativa.

   Ejemplo desde la raiz del repo de analisis:
   ```bash
   git clone https://github.com/djwillichile/chilecompra-data-processor /tmp/proc
   cp -r /tmp/proc/examples/consumer-repo/scripts ./
   mkdir -p .github/workflows
   cp /tmp/proc/examples/consumer-repo/.github/workflows/sync-data.yml .github/workflows/
   chmod +x scripts/download_data.sh
   ```

2. **Asegurarse de que `data/processed/` no este en `.gitignore`** del repo de analisis si se quiere versionar los datos. Por defecto el workflow hace commit a `main`.

3. **Probar el script localmente** (requiere [gh CLI](https://cli.github.com/) autenticado):
   ```bash
   ./scripts/download_data.sh
   ```

4. **Probar el workflow** desde la pestaña *Actions* del repo de analisis: *"Sincronizar datos ChileCompra"* -> *"Run workflow"*.

## Notas

- El workflow se ejecuta automaticamente cada **1 del mes a las 06:00 UTC**. Cambia
  el `cron` segun lo necesites.
- Si se prefiere no commitear los datos al repo (mantenerlo limpio), eliminar los
  pasos *"Detectar cambios"* y *"Commit y push"* del workflow y agregar
  `data/processed/` al `.gitignore` del repo de analisis. Quien quiera los datos
  los descarga con `./scripts/download_data.sh` cuando los necesite.
- El `GITHUB_TOKEN` automatico tiene permisos suficientes para leer releases del
  repositorio publico `chilecompra-data-processor` y para hacer push al propio
  repo de analisis. No se necesita un PAT.
