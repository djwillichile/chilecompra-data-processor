# Documentacion para mantenedores

Esta guia describe el setup interno del workflow `procesar-datos.yml` y como
configurarlo para distribuir automaticamente los Parquet generados al
repositorio de analisis. Aplica solo si eres el mantenedor del repositorio
o si forkeaste el proyecto y quieres replicar el flujo completo.

## Sincronizacion automatica al repo de analisis (push cross-repo)

El workflow `procesar-datos.yml` puede empujar los Parquet directamente al
repositorio
[chile-public-procurement-analysis](https://github.com/djwillichile/chile-public-procurement-analysis)
al finalizar cada corrida. El step solo se ejecuta si el secret
`ANALYSIS_REPO_TOKEN` esta configurado. Si no esta, el workflow se comporta
como antes y solo publica el release.

### Configuracion

1. **Crear un Personal Access Token** (Fine-grained recomendado) con
   permisos `Contents: Read and write` sobre `chile-public-procurement-analysis`.
   GitHub -> Settings -> Developer settings -> Personal access tokens.

2. **Guardar el token como secret** en este repositorio
   (Settings -> Secrets and variables -> Actions -> *New repository secret*) con
   el nombre exacto `ANALYSIS_REPO_TOKEN`.

3. **Listo.** En la siguiente corrida de "Procesar datos ChileCompra", al
   terminar de publicar el release, el workflow hara `git clone` del repo de
   analisis, copiara los Parquet a `data/processed/`, y hara commit + push como
   `djwillichile` solo si hay cambios reales.

### Que hace el workflow

Resumen de los steps relevantes en `.github/workflows/procesar-datos.yml`:

1. Procesa los datos de ChileCompra y genera los 7 archivos en `data/processed/`.
2. Publica un release con tag `data-YYYY-MM-DD` y los archivos como assets.
3. Si `ANALYSIS_REPO_TOKEN` esta presente:
   - Clona `chile-public-procurement-analysis` con el token.
   - Copia `data/processed/*.parquet` y `*.json`.
   - Commitea y pushea solo si hay cambios reales (detectados con
     `git diff --cached --quiet`).

## Alternativa: pull desde el repo de analisis

Para forks u otros consumidores que prefieran extraer los datos en lugar de
recibirlos por push, en `examples/consumer-repo/` hay un script de descarga
(`download_data.sh`) y un workflow de sincronizacion (`sync-data.yml`) listos
para copiarse al repo destino. Ver
[examples/consumer-repo/README.md](../examples/consumer-repo/README.md) para
el detalle de instalacion.
