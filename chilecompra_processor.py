#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================
  ChileCompra Data Processor v2.0 - Windows/Linux/macOS
  Descarga y procesamiento de licitaciones 2015-2026
  Autor: Guillermo Fuentes (djwillichile)
==============================================================

Caracteristicas:
  - Guardado incremental: cada mes procesado se guarda inmediatamente.
  - Reanudacion automatica: detecta meses ya procesados y los salta.
  - Interrupcion limpia: Ctrl+C guarda el estado actual antes de salir.
  - Progreso visible: muestra avance, tiempo estimado y estadisticas.

Uso:
    python chilecompra_processor.py          # Ejecutar normalmente
    python chilecompra_processor.py --reset   # Reiniciar desde cero

Requisitos:
    pip install pandas pyarrow requests
"""

import os
import sys
import signal
import zipfile
import requests
import pandas as pd
import json
import time
from pathlib import Path
from datetime import timedelta

# ==========================================
# CONFIGURACION
# ==========================================
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = BASE_DIR / "data" / "raw" / "licitaciones"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROGRESS_FILE = PROCESSED_DIR / "_progress.json"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://transparenciachc.blob.core.windows.net/lic-da"
START_YEAR = 2015
END_YEAR = 2026

# Columnas que necesitamos extraer de cada CSV
USECOLS = [
    'CodigoExterno', 'NombreOrganismo', 'sector', 'RegionUnidad',
    'Tipo', 'Estado', 'MontoEstimado', 'FechaPublicacion', 'FechaAdjudicacion',
    'NumeroOferentes', 'Rubro1', 'NombreProveedor', 'MontoLineaAdjudica'
]

# Estado global
interrupted = False
total_licitaciones_session = 0


# ==========================================
# MANEJO DE INTERRUPCIONES
# ==========================================
def signal_handler(sig, frame):
    """Captura Ctrl+C para salir limpiamente."""
    global interrupted
    print("\n")
    print("=" * 60)
    print("  [!] Interrupcion detectada (Ctrl+C)")
    print("  [!] Guardando estado actual...")
    print("  [!] Puedes reanudar ejecutando el script nuevamente.")
    print("=" * 60)
    interrupted = True


signal.signal(signal.SIGINT, signal_handler)


# ==========================================
# GESTION DE PROGRESO
# ==========================================
def load_progress():
    """Carga el registro de meses ya procesados."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"processed_months": [], "total_records": 0, "last_update": ""}


def save_progress(progress):
    """Guarda el registro de progreso."""
    progress["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def reset_progress():
    """Elimina todo el progreso y datos procesados."""
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
    for f in PROCESSED_DIR.glob("*.parquet"):
        f.unlink()
    for f in PROCESSED_DIR.glob("*.json"):
        if f.name != "_progress.json":
            f.unlink()
    print("  [OK] Progreso y datos procesados eliminados.")


# ==========================================
# DESCARGA Y EXTRACCION
# ==========================================
def download_file(url, dest_path, max_retries=3):
    """Descarga un archivo con reintentos."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=300)
            if response.status_code == 200:
                with open(dest_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            elif response.status_code == 404:
                return False
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                print(f"retry {attempt+1}...", end=" ", flush=True)
                time.sleep(5)
            else:
                print(f"ERROR: {e}")
                return False
    return False


def extract_zip(zip_path, dest_dir):
    """Extrae un archivo ZIP y devuelve la ruta del CSV extraido."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if csv_names:
                zf.extract(csv_names[0], dest_dir)
                return dest_dir / csv_names[0]
    except zipfile.BadZipFile:
        print("[ZIP corrupto]", end=" ", flush=True)
    return None


# ==========================================
# PROCESAMIENTO DE CSV
# ==========================================
def process_csv(csv_path, year, month):
    """Procesa un CSV en chunks y devuelve DataFrames agregados."""
    global total_licitaciones_session

    chunks_cat = []
    chunks_org = []
    chunks_reg = []
    chunks_prov = []
    chunks_comp = []
    records_in_file = 0

    try:
        for chunk in pd.read_csv(
            csv_path, sep=';', encoding='latin-1',
            usecols=USECOLS, dtype=str,
            chunksize=100000, on_bad_lines='skip', quoting=1
        ):
            if interrupted:
                break

            # Convertir montos a numerico
            chunk['MontoLineaAdjudica'] = pd.to_numeric(chunk['MontoLineaAdjudica'], errors='coerce').fillna(0)
            chunk['MontoEstimado'] = pd.to_numeric(chunk['MontoEstimado'], errors='coerce').fillna(0)
            chunk['NumeroOferentes'] = pd.to_numeric(chunk['NumeroOferentes'], errors='coerce').fillna(0)

            # Monto efectivo: adjudicado si existe, sino estimado
            chunk['monto'] = chunk['MontoLineaAdjudica'].where(
                chunk['MontoLineaAdjudica'] > 0, chunk['MontoEstimado']
            )

            records_in_file += len(chunk)
            total_licitaciones_session += len(chunk)

            # --- Agregado por categoria ---
            cat_agg = chunk.groupby('Rubro1').agg(
                n_licitaciones=('CodigoExterno', 'count'),
                monto_adjudicado=('monto', 'sum'),
                oferentes_promedio=('NumeroOferentes', 'mean'),
                n_proveedores=('NombreProveedor', 'nunique'),
                n_organismos=('NombreOrganismo', 'nunique')
            ).reset_index()
            cat_agg['anio'] = year
            cat_agg['mes'] = month
            chunks_cat.append(cat_agg)

            # --- Agregado por organismo ---
            org_agg = chunk.groupby(['NombreOrganismo', 'sector']).agg(
                n_licitaciones=('CodigoExterno', 'count'),
                monto_total=('monto', 'sum'),
                oferentes_promedio=('NumeroOferentes', 'mean')
            ).reset_index()
            org_agg['anio'] = year
            chunks_org.append(org_agg)

            # --- Agregado por region ---
            reg_agg = chunk.groupby('RegionUnidad').agg(
                n_licitaciones=('CodigoExterno', 'count'),
                monto_total=('monto', 'sum'),
                n_organismos=('NombreOrganismo', 'nunique')
            ).reset_index()
            reg_agg['anio'] = year
            chunks_reg.append(reg_agg)

            # --- Agregado por proveedor ---
            prov_mask = chunk['NombreProveedor'].notna() & (chunk['NombreProveedor'] != '')
            prov_agg = chunk[prov_mask].groupby('NombreProveedor').agg(
                n_adjudicaciones=('CodigoExterno', 'count'),
                monto_total=('monto', 'sum'),
                n_rubros=('Rubro1', 'nunique')
            ).reset_index()
            prov_agg['anio'] = year
            chunks_prov.append(prov_agg)

            # --- Competencia por rubro ---
            comp_agg = chunk.groupby('Rubro1').agg(
                oferentes_promedio=('NumeroOferentes', 'mean'),
                oferentes_mediana=('NumeroOferentes', 'median'),
                n_licitaciones=('CodigoExterno', 'count')
            ).reset_index()
            comp_agg['anio'] = year
            chunks_comp.append(comp_agg)

    except Exception as e:
        print(f"  Error procesando: {e}")
        return None, None, None, None, None, 0

    if interrupted:
        return None, None, None, None, None, 0

    df_cat = pd.concat(chunks_cat, ignore_index=True) if chunks_cat else pd.DataFrame()
    df_org = pd.concat(chunks_org, ignore_index=True) if chunks_org else pd.DataFrame()
    df_reg = pd.concat(chunks_reg, ignore_index=True) if chunks_reg else pd.DataFrame()
    df_prov = pd.concat(chunks_prov, ignore_index=True) if chunks_prov else pd.DataFrame()
    df_comp = pd.concat(chunks_comp, ignore_index=True) if chunks_comp else pd.DataFrame()

    return df_cat, df_org, df_reg, df_prov, df_comp, records_in_file


# ==========================================
# GUARDADO INCREMENTAL
# ==========================================
def save_incremental(df_new, filename):
    """Agrega nuevos datos al Parquet existente (append incremental)."""
    filepath = PROCESSED_DIR / filename
    if filepath.exists():
        df_existing = pd.read_parquet(filepath)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    df_combined.to_parquet(filepath, index=False)
    return len(df_combined)


# ==========================================
# CONSOLIDACION FINAL
# ==========================================
def consolidate_final():
    """Genera los archivos finales consolidados (metricas, sankey, etc.)."""
    print("\n  Generando archivos consolidados finales...")

    # Metricas anuales por categoria con indice de oportunidad
    cat_path = PROCESSED_DIR / 'agregado_mensual_categoria.parquet'
    if cat_path.exists():
        df_cat = pd.read_parquet(cat_path)
        df_anual_cat = df_cat.groupby(['anio', 'Rubro1']).agg(
            monto_adjudicado=('monto_adjudicado', 'sum'),
            n_licitaciones=('n_licitaciones', 'sum'),
            oferentes_promedio=('oferentes_promedio', 'mean')
        ).reset_index()

        df_anual_cat = df_anual_cat.sort_values(['Rubro1', 'anio'])
        df_anual_cat['crecimiento_monto_pct'] = df_anual_cat.groupby('Rubro1')['monto_adjudicado'].pct_change() * 100
        df_anual_cat['crecimiento_lic_pct'] = df_anual_cat.groupby('Rubro1')['n_licitaciones'].pct_change() * 100

        latest = df_anual_cat[df_anual_cat['anio'] == df_anual_cat['anio'].max()].copy()
        if len(latest) > 0:
            latest['monto_norm'] = (latest['monto_adjudicado'] - latest['monto_adjudicado'].min()) / \
                                   (latest['monto_adjudicado'].max() - latest['monto_adjudicado'].min() + 1)
            latest['crec_norm'] = (latest['crecimiento_monto_pct'] - latest['crecimiento_monto_pct'].min()) / \
                                  (latest['crecimiento_monto_pct'].max() - latest['crecimiento_monto_pct'].min() + 1)
            latest['comp_norm'] = 1 - (latest['oferentes_promedio'] - latest['oferentes_promedio'].min()) / \
                                      (latest['oferentes_promedio'].max() - latest['oferentes_promedio'].min() + 1)
            latest['indice_oportunidad'] = (latest['monto_norm'] * 0.4 +
                                            latest['crec_norm'] * 0.35 +
                                            latest['comp_norm'] * 0.25)
            df_anual_cat = df_anual_cat.merge(latest[['Rubro1', 'indice_oportunidad']], on='Rubro1', how='left')

        out = PROCESSED_DIR / 'metricas_anuales_categoria.parquet'
        df_anual_cat.to_parquet(out, index=False)
        print(f"    [OK] {out.name}: {len(df_anual_cat):,} filas")

    # Sankey data 2024
    org_path = PROCESSED_DIR / 'agregado_anual_organismo_clean.parquet'
    reg_path = PROCESSED_DIR / 'agregado_anual_region_clean.parquet'
    prov_path = PROCESSED_DIR / 'agregado_anual_proveedor_top500.parquet'

    if cat_path.exists() and org_path.exists() and reg_path.exists() and prov_path.exists():
        df_org = pd.read_parquet(org_path)
        df_reg = pd.read_parquet(reg_path)
        df_prov = pd.read_parquet(prov_path)
        df_cat_raw = pd.read_parquet(cat_path)

        sankey = {'organismos': [], 'categorias': [], 'proveedores': [], 'regiones': []}

        org_2024 = df_org[df_org['anio'] == 2024].groupby('NombreOrganismo')['monto_total'].sum().nlargest(15)
        sankey['organismos'] = [{'NombreOrganismo': k, 'monto_total': float(v)} for k, v in org_2024.items()]

        cat_2024 = df_cat_raw[df_cat_raw['anio'] == 2024].groupby('Rubro1')['monto_adjudicado'].sum().nlargest(15)
        sankey['categorias'] = [{'Rubro1': k, 'monto_adjudicado': float(v)} for k, v in cat_2024.items()]

        prov_2024 = df_prov[df_prov['anio'] == 2024].groupby('NombreProveedor')['monto_total'].sum().nlargest(15)
        sankey['proveedores'] = [{'NombreProveedor': k, 'monto_total': float(v)} for k, v in prov_2024.items()]

        reg_2024 = df_reg[df_reg['anio'] == 2024].groupby('RegionUnidad')['monto_total'].sum().nlargest(15)
        sankey['regiones'] = [{'RegionUnidad': k, 'monto_total': float(v)} for k, v in reg_2024.items()]

        out = PROCESSED_DIR / 'sankey_tops_2024.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(sankey, f, ensure_ascii=False, indent=2)
        print(f"    [OK] {out.name}: saved")


# ==========================================
# MAIN
# ==========================================
def main():
    global interrupted

    print("=" * 60)
    print("  ChileCompra Data Processor v2.0")
    print("  Descarga y procesamiento de licitaciones 2015-2026")
    print("=" * 60)
    print(f"  Directorio: {BASE_DIR}")
    print(f"  Procesados: {PROCESSED_DIR}")
    print()
    print("  Controles:")
    print("    Ctrl+C  = Guardar progreso y salir limpiamente")
    print("    --reset = Reiniciar desde cero (borrar todo)")
    print("=" * 60)
    print()

    # Verificar flag --reset
    if "--reset" in sys.argv:
        print("  [!] Reiniciando desde cero...")
        reset_progress()
        print()

    # Cargar progreso
    progress = load_progress()
    processed_months = set(progress["processed_months"])

    # Calcular total de meses a procesar
    all_months = []
    for year in range(START_YEAR, END_YEAR + 1):
        max_month = 4 if year == 2026 else 12
        for month in range(1, max_month + 1):
            all_months.append(f"{year}-{month}")

    total_months = len(all_months)
    already_done = len(processed_months)
    pending = total_months - already_done

    if already_done > 0:
        print(f"  [REANUDANDO] {already_done}/{total_months} meses ya procesados.")
        print(f"  [PENDIENTE]  {pending} meses por procesar.")
        print(f"  [REGISTROS]  {progress['total_records']:,} registros acumulados.")
        print()

    if pending == 0:
        print("  [OK] Todos los meses ya fueron procesados.")
        print("  Generando archivos consolidados finales...")
        consolidate_final()
        print()
        print("  COMPLETADO. Los archivos estan en:")
        print(f"  {PROCESSED_DIR}")
        print()
        input("  Presiona ENTER para cerrar...")
        return

    start_time = time.time()
    months_processed_this_session = 0

    for tag in all_months:
        if interrupted:
            break

        # Saltar meses ya procesados
        if tag in processed_months:
            continue

        year, month = map(int, tag.split('-'))
        csv_path = RAW_DIR / f"lic_{tag}.csv"
        zip_path = RAW_DIR / f"lic_{tag}.zip"
        url = f"{BASE_URL}/{tag}.zip"

        # Progreso visual
        done_so_far = already_done + months_processed_this_session
        pct = (done_so_far / total_months) * 100
        elapsed = time.time() - start_time
        if months_processed_this_session > 0:
            avg_time = elapsed / months_processed_this_session
            eta_seconds = avg_time * (pending - months_processed_this_session)
            eta_str = str(timedelta(seconds=int(eta_seconds)))
        else:
            eta_str = "calculando..."

        print(f"  [{done_so_far+1}/{total_months}] ({pct:.0f}%) ETA: {eta_str} | {tag} ", end="", flush=True)

        # --- Descargar ---
        if not (csv_path.exists() and csv_path.stat().st_size > 1000):
            print("descargando...", end=" ", flush=True)

            if not download_file(url, zip_path):
                print("NO DISPONIBLE - skip")
                # Marcar como procesado para no reintentar
                progress["processed_months"].append(tag)
                save_progress(progress)
                months_processed_this_session += 1
                continue

            size_mb = zip_path.stat().st_size / (1024 * 1024)
            print(f"({size_mb:.1f}MB)", end=" ", flush=True)

            # --- Extraer ---
            extracted = extract_zip(zip_path, RAW_DIR)
            zip_path.unlink(missing_ok=True)

            if extracted is None:
                print("EXTRACTION FAILED - skip")
                progress["processed_months"].append(tag)
                save_progress(progress)
                months_processed_this_session += 1
                continue

            # Renombrar al nombre estandar
            if extracted != csv_path:
                extracted.rename(csv_path)

        # --- Procesar ---
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        print(f"procesando ({csv_size:.0f}MB)...", end=" ", flush=True)

        df_cat, df_org, df_reg, df_prov, df_comp, records = process_csv(str(csv_path), year, month)

        if interrupted:
            # No marcar como procesado si fue interrumpido a mitad del archivo
            csv_path.unlink(missing_ok=True)
            break

        if df_cat is not None and len(df_cat) > 0:
            # --- GUARDADO INCREMENTAL ---
            save_incremental(df_cat, 'agregado_mensual_categoria.parquet')
            save_incremental(df_org, 'agregado_anual_organismo_clean.parquet')
            save_incremental(df_reg, 'agregado_anual_region_clean.parquet')
            save_incremental(df_comp, 'competencia_por_rubro.parquet')

            # Proveedores: guardar y mantener top 500 global
            prov_path = PROCESSED_DIR / 'agregado_anual_proveedor_top500.parquet'
            if prov_path.exists():
                df_prov_existing = pd.read_parquet(prov_path)
                df_prov_all = pd.concat([df_prov_existing, df_prov], ignore_index=True)
            else:
                df_prov_all = df_prov
            # Re-agregar y mantener top 500
            df_prov_agg = df_prov_all.groupby(['anio', 'NombreProveedor']).agg(
                n_adjudicaciones=('n_adjudicaciones', 'sum'),
                monto_total=('monto_total', 'sum'),
                n_rubros=('n_rubros', 'max')
            ).reset_index()
            top_provs = df_prov_agg.groupby('NombreProveedor')['monto_total'].sum().nlargest(500).index
            df_prov_final = df_prov_agg[df_prov_agg['NombreProveedor'].isin(top_provs)]
            df_prov_final.to_parquet(prov_path, index=False)

            # Actualizar progreso
            progress["processed_months"].append(tag)
            progress["total_records"] += records
            save_progress(progress)

            months_processed_this_session += 1
            print(f"OK ({records:,} registros) [GUARDADO]")
        else:
            progress["processed_months"].append(tag)
            save_progress(progress)
            months_processed_this_session += 1
            print("EMPTY - skip [GUARDADO]")

        # Eliminar CSV crudo para ahorrar espacio
        csv_path.unlink(missing_ok=True)

    # --- RESUMEN FINAL ---
    elapsed_total = time.time() - start_time
    minutes = int(elapsed_total // 60)
    seconds = int(elapsed_total % 60)

    print()
    print("=" * 60)
    if interrupted:
        print("  PROCESO INTERRUMPIDO - PROGRESO GUARDADO")
        print(f"  Meses procesados en esta sesion: {months_processed_this_session}")
        print(f"  Total acumulado: {len(progress['processed_months'])}/{total_months}")
        print()
        print("  Para reanudar, ejecuta el script nuevamente.")
        print("  Para reiniciar desde cero:")
        print("    python chilecompra_processor.py --reset")
    else:
        print("  PROCESAMIENTO COMPLETO")
        # Generar archivos consolidados finales
        consolidate_final()

    print()
    print(f"  Registros esta sesion: {total_licitaciones_session:,}")
    print(f"  Registros acumulados:  {progress['total_records']:,}")
    print(f"  Tiempo esta sesion:    {minutes}m {seconds}s")
    print(f"  Datos en:              {PROCESSED_DIR}")
    print("=" * 60)
    print()
    input("  Presiona ENTER para cerrar...")


if __name__ == '__main__':
    main()
