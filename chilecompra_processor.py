#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================
  ChileCompra Data Processor v3.0 - Windows/Linux/macOS
  Descarga y procesamiento de licitaciones 2015-2026
  Autor: Guillermo Fuentes (djwillichile)
==============================================================

Uso:
    python chilecompra_processor.py          # Ejecutar (o reanudar)
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
MONTHLY_DIR = PROCESSED_DIR / "monthly"
PROGRESS_FILE = PROCESSED_DIR / "_progress.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
MONTHLY_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://transparenciachc.blob.core.windows.net/lic-da"
START_YEAR = 2015
END_YEAR = 2026

IS_INTERACTIVE = sys.stdin.isatty()

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
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"processed_months": [], "total_records": 0, "last_update": ""}


def save_progress(progress):
    progress["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def reset_progress():
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
    for f in PROCESSED_DIR.glob("*.parquet"):
        f.unlink()
    for f in PROCESSED_DIR.glob("*.json"):
        if f.name != "_progress.json":
            f.unlink()
    for d in MONTHLY_DIR.iterdir():
        for f in d.glob("*.parquet"):
            f.unlink()
        d.rmdir()
    print("  [OK] Progreso y datos procesados eliminados.")


# ==========================================
# DESCARGA Y EXTRACCION
# ==========================================
def download_file(url, dest_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=300)
            if response.status_code == 200:
                with open(dest_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        f.write(chunk)
                return True
            elif response.status_code == 404:
                return False
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"retry {attempt+1} (espera {wait}s)...", end=" ", flush=True)
                time.sleep(wait)
            else:
                print(f"ERROR: {e}")
                return False
    return False


def extract_zip(zip_path, dest_dir):
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
# PROCESAMIENTO - AGREGACION MENSUAL CORRECTA
# ==========================================
def process_csv(csv_path, year, month):
    """
    Procesa un CSV por chunks y devuelve DataFrames agregados al nivel mensual.

    Estrategia para valores combinables entre chunks:
      - Sumas (monto, n_licitaciones): acumulacion directa.
      - Promedios (oferentes_promedio): acumula sum+count, calcula media al final.
      - Unicidad (n_proveedores, n_organismos, n_rubros): acumula sets, len() al final.
      - Mediana (oferentes_mediana): acumula lista de valores, calcula al final.
    """
    global total_licitaciones_session

    # Acumuladores por tipo de agregado
    # Categoria: key = Rubro1
    acc_cat = {}
    # Organismo: key = (NombreOrganismo, sector)
    acc_org = {}
    # Region: key = RegionUnidad
    acc_reg = {}
    # Proveedor: key = NombreProveedor
    acc_prov = {}
    # Competencia: key = Rubro1
    acc_comp = {}

    records_in_file = 0

    try:
        for chunk in pd.read_csv(
            csv_path, sep=';', encoding='latin-1',
            usecols=USECOLS, dtype=str,
            chunksize=100_000, on_bad_lines='skip', quoting=1
        ):
            if interrupted:
                break

            chunk['MontoLineaAdjudica'] = pd.to_numeric(chunk['MontoLineaAdjudica'], errors='coerce').fillna(0)
            chunk['MontoEstimado'] = pd.to_numeric(chunk['MontoEstimado'], errors='coerce').fillna(0)
            chunk['NumeroOferentes'] = pd.to_numeric(chunk['NumeroOferentes'], errors='coerce').fillna(0)

            chunk['monto'] = chunk['MontoLineaAdjudica'].where(
                chunk['MontoLineaAdjudica'] > 0, chunk['MontoEstimado']
            )
            chunk['Rubro1'] = chunk['Rubro1'].fillna('Sin categoria')
            chunk['NombreOrganismo'] = chunk['NombreOrganismo'].fillna('Sin organismo')
            chunk['sector'] = chunk['sector'].fillna('Sin sector')
            chunk['RegionUnidad'] = chunk['RegionUnidad'].fillna('Sin region')

            records_in_file += len(chunk)
            total_licitaciones_session += len(chunk)

            # --- CATEGORIA ---
            for rubro, grp in chunk.groupby('Rubro1', sort=False):
                if rubro not in acc_cat:
                    acc_cat[rubro] = {
                        'n_lic': 0, 'monto_sum': 0.0,
                        'ofert_sum': 0.0, 'ofert_count': 0,
                        'provs': set(), 'orgs': set()
                    }
                d = acc_cat[rubro]
                d['n_lic'] += len(grp)
                d['monto_sum'] += grp['monto'].sum()
                ofert = grp['NumeroOferentes']
                d['ofert_sum'] += ofert.sum()
                d['ofert_count'] += len(ofert)
                d['provs'].update(grp['NombreProveedor'].dropna().unique())
                d['orgs'].update(grp['NombreOrganismo'].dropna().unique())

            # --- ORGANISMO ---
            for (org, sec), grp in chunk.groupby(['NombreOrganismo', 'sector'], sort=False):
                key = (org, sec)
                if key not in acc_org:
                    acc_org[key] = {'n_lic': 0, 'monto_sum': 0.0, 'ofert_sum': 0.0, 'ofert_count': 0}
                d = acc_org[key]
                d['n_lic'] += len(grp)
                d['monto_sum'] += grp['monto'].sum()
                ofert = grp['NumeroOferentes']
                d['ofert_sum'] += ofert.sum()
                d['ofert_count'] += len(ofert)

            # --- REGION ---
            for region, grp in chunk.groupby('RegionUnidad', sort=False):
                if region not in acc_reg:
                    acc_reg[region] = {'n_lic': 0, 'monto_sum': 0.0, 'orgs': set()}
                d = acc_reg[region]
                d['n_lic'] += len(grp)
                d['monto_sum'] += grp['monto'].sum()
                d['orgs'].update(grp['NombreOrganismo'].dropna().unique())

            # --- PROVEEDOR ---
            prov_mask = chunk['NombreProveedor'].notna() & (chunk['NombreProveedor'] != '')
            for prov, grp in chunk[prov_mask].groupby('NombreProveedor', sort=False):
                if prov not in acc_prov:
                    acc_prov[prov] = {'n_adj': 0, 'monto_sum': 0.0, 'rubros': set()}
                d = acc_prov[prov]
                d['n_adj'] += len(grp)
                d['monto_sum'] += grp['monto'].sum()
                d['rubros'].update(grp['Rubro1'].dropna().unique())

            # --- COMPETENCIA ---
            for rubro, grp in chunk.groupby('Rubro1', sort=False):
                if rubro not in acc_comp:
                    acc_comp[rubro] = {'ofert_vals': [], 'n_lic': 0}
                d = acc_comp[rubro]
                d['ofert_vals'].extend(grp['NumeroOferentes'].tolist())
                d['n_lic'] += len(grp)

    except Exception as e:
        print(f"  Error procesando: {e}")
        return None, None, None, None, None, 0

    if interrupted:
        return None, None, None, None, None, 0

    # --- Construir DataFrames con valores correctamente combinados ---

    df_cat = pd.DataFrame([
        {
            'anio': year, 'mes': month, 'Rubro1': rubro,
            'n_licitaciones': d['n_lic'],
            'monto_adjudicado': d['monto_sum'],
            'oferentes_promedio': d['ofert_sum'] / d['ofert_count'] if d['ofert_count'] else 0.0,
            'n_proveedores': len(d['provs']),
            'n_organismos': len(d['orgs']),
        }
        for rubro, d in acc_cat.items()
    ])

    df_org = pd.DataFrame([
        {
            'anio': year, 'NombreOrganismo': org, 'sector': sec,
            'n_licitaciones': d['n_lic'],
            'monto_total': d['monto_sum'],
            'oferentes_promedio': d['ofert_sum'] / d['ofert_count'] if d['ofert_count'] else 0.0,
        }
        for (org, sec), d in acc_org.items()
    ])

    df_reg = pd.DataFrame([
        {
            'anio': year, 'RegionUnidad': region,
            'n_licitaciones': d['n_lic'],
            'monto_total': d['monto_sum'],
            'n_organismos': len(d['orgs']),
        }
        for region, d in acc_reg.items()
    ])

    df_prov = pd.DataFrame([
        {
            'anio': year, 'NombreProveedor': prov,
            'n_adjudicaciones': d['n_adj'],
            'monto_total': d['monto_sum'],
            'n_rubros': len(d['rubros']),
        }
        for prov, d in acc_prov.items()
    ])

    df_comp = pd.DataFrame([
        {
            'anio': year, 'Rubro1': rubro,
            'n_licitaciones': d['n_lic'],
            'oferentes_promedio': sum(d['ofert_vals']) / len(d['ofert_vals']) if d['ofert_vals'] else 0.0,
            'oferentes_mediana': float(pd.Series(d['ofert_vals']).median()) if d['ofert_vals'] else 0.0,
        }
        for rubro, d in acc_comp.items()
    ])

    return df_cat, df_org, df_reg, df_prov, df_comp, records_in_file


# ==========================================
# GUARDADO MENSUAL - UN DIRECTORIO POR MES
# ==========================================
def save_monthly(df_cat, df_org, df_reg, df_prov, df_comp, tag):
    """Guarda los 5 agregados de un mes, cada uno en su propio parquet."""
    month_dir = MONTHLY_DIR / tag
    month_dir.mkdir(exist_ok=True)
    df_cat.to_parquet(month_dir / 'cat.parquet', index=False)
    df_org.to_parquet(month_dir / 'org.parquet', index=False)
    df_reg.to_parquet(month_dir / 'reg.parquet', index=False)
    df_prov.to_parquet(month_dir / 'prov.parquet', index=False)
    df_comp.to_parquet(month_dir / 'comp.parquet', index=False)


# ==========================================
# CONSOLIDACION FINAL
# ==========================================
def consolidate_final():
    """
    Lee todos los archivos mensuales y genera los 7 archivos finales consolidados.
    Esta funcion se ejecuta una sola vez al finalizar todo el procesamiento.
    """
    print("\n  Generando archivos consolidados finales...")

    month_dirs = sorted(MONTHLY_DIR.iterdir())
    if not month_dirs:
        print("  [!] No hay datos mensuales para consolidar.")
        return

    cats, orgs, regs, provs, comps = [], [], [], [], []
    for d in month_dirs:
        if (d / 'cat.parquet').exists():
            cats.append(pd.read_parquet(d / 'cat.parquet'))
        if (d / 'org.parquet').exists():
            orgs.append(pd.read_parquet(d / 'org.parquet'))
        if (d / 'reg.parquet').exists():
            regs.append(pd.read_parquet(d / 'reg.parquet'))
        if (d / 'prov.parquet').exists():
            provs.append(pd.read_parquet(d / 'prov.parquet'))
        if (d / 'comp.parquet').exists():
            comps.append(pd.read_parquet(d / 'comp.parquet'))

    # --- Categoria mensual (granularidad original: anio+mes+Rubro1) ---
    df_cat = pd.concat(cats, ignore_index=True)
    out = PROCESSED_DIR / 'agregado_mensual_categoria.parquet'
    df_cat.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_cat):,} filas")

    # --- Organismo anual (re-agregar meses -> año) ---
    df_org = pd.concat(orgs, ignore_index=True)
    df_org_anual = df_org.groupby(['anio', 'NombreOrganismo', 'sector']).agg(
        n_licitaciones=('n_licitaciones', 'sum'),
        monto_total=('monto_total', 'sum'),
        oferentes_promedio=('oferentes_promedio', 'mean')
    ).reset_index()
    out = PROCESSED_DIR / 'agregado_anual_organismo_clean.parquet'
    df_org_anual.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_org_anual):,} filas")

    # --- Region anual ---
    df_reg = pd.concat(regs, ignore_index=True)
    df_reg_anual = df_reg.groupby(['anio', 'RegionUnidad']).agg(
        n_licitaciones=('n_licitaciones', 'sum'),
        monto_total=('monto_total', 'sum'),
        n_organismos=('n_organismos', 'sum')
    ).reset_index()
    out = PROCESSED_DIR / 'agregado_anual_region_clean.parquet'
    df_reg_anual.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_reg_anual):,} filas")

    # --- Competencia por rubro (anual) ---
    df_comp = pd.concat(comps, ignore_index=True)
    df_comp_anual = df_comp.groupby(['anio', 'Rubro1']).agg(
        n_licitaciones=('n_licitaciones', 'sum'),
        oferentes_promedio=('oferentes_promedio', 'mean'),
        oferentes_mediana=('oferentes_mediana', 'mean')
    ).reset_index()
    out = PROCESSED_DIR / 'competencia_por_rubro.parquet'
    df_comp_anual.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_comp_anual):,} filas")

    # --- Proveedor top 500 (re-agregar por año, filtrar al final) ---
    df_prov = pd.concat(provs, ignore_index=True)
    df_prov_anual = df_prov.groupby(['anio', 'NombreProveedor']).agg(
        n_adjudicaciones=('n_adjudicaciones', 'sum'),
        monto_total=('monto_total', 'sum'),
        n_rubros=('n_rubros', 'max')
    ).reset_index()
    top_provs = df_prov_anual.groupby('NombreProveedor')['monto_total'].sum().nlargest(500).index
    df_prov_top = df_prov_anual[df_prov_anual['NombreProveedor'].isin(top_provs)]
    out = PROCESSED_DIR / 'agregado_anual_proveedor_top500.parquet'
    df_prov_top.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_prov_top):,} filas")

    # --- Metricas anuales por categoria con indice de oportunidad ---
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
        def _norm(s):
            rng = s.max() - s.min()
            return (s - s.min()) / (rng + 1e-9)
        latest['monto_norm'] = _norm(latest['monto_adjudicado'])
        latest['crec_norm'] = _norm(latest['crecimiento_monto_pct'].fillna(0))
        latest['comp_norm'] = 1 - _norm(latest['oferentes_promedio'])
        latest['indice_oportunidad'] = (latest['monto_norm'] * 0.4 +
                                        latest['crec_norm'] * 0.35 +
                                        latest['comp_norm'] * 0.25)
        df_anual_cat = df_anual_cat.merge(
            latest[['Rubro1', 'indice_oportunidad']], on='Rubro1', how='left'
        )
    out = PROCESSED_DIR / 'metricas_anuales_categoria.parquet'
    df_anual_cat.to_parquet(out, index=False)
    print(f"    [OK] {out.name}: {len(df_anual_cat):,} filas")

    # --- Sankey: top 15 por nivel del año mas reciente disponible ---
    max_anio = df_cat['anio'].max()
    sankey = {'organismos': [], 'categorias': [], 'proveedores': [], 'regiones': []}

    org_top = df_org_anual[df_org_anual['anio'] == max_anio].groupby('NombreOrganismo')['monto_total'].sum().nlargest(15)
    sankey['organismos'] = [{'NombreOrganismo': k, 'monto_total': float(v)} for k, v in org_top.items()]

    cat_top = df_cat[df_cat['anio'] == max_anio].groupby('Rubro1')['monto_adjudicado'].sum().nlargest(15)
    sankey['categorias'] = [{'Rubro1': k, 'monto_adjudicado': float(v)} for k, v in cat_top.items()]

    prov_top = df_prov_anual[df_prov_anual['anio'] == max_anio].groupby('NombreProveedor')['monto_total'].sum().nlargest(15)
    sankey['proveedores'] = [{'NombreProveedor': k, 'monto_total': float(v)} for k, v in prov_top.items()]

    reg_top = df_reg_anual[df_reg_anual['anio'] == max_anio].groupby('RegionUnidad')['monto_total'].sum().nlargest(15)
    sankey['regiones'] = [{'RegionUnidad': k, 'monto_total': float(v)} for k, v in reg_top.items()]

    out = PROCESSED_DIR / 'sankey_tops_2024.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(sankey, f, ensure_ascii=False, indent=2)
    print(f"    [OK] {out.name}: guardado (año {max_anio})")


# ==========================================
# MAIN
# ==========================================
def main():
    global interrupted

    print("=" * 60)
    print("  ChileCompra Data Processor v3.0")
    print("  Descarga y procesamiento de licitaciones 2015-2026")
    print("=" * 60)
    print(f"  Directorio: {BASE_DIR}")
    print(f"  Procesados: {PROCESSED_DIR}")
    print()
    if IS_INTERACTIVE:
        print("  Controles:")
        print("    Ctrl+C  = Guardar progreso y salir limpiamente")
        print("    --reset = Reiniciar desde cero (borrar todo)")
    print("=" * 60)
    print()

    if "--reset" in sys.argv:
        print("  [!] Reiniciando desde cero...")
        reset_progress()
        print()

    progress = load_progress()
    processed_months = set(progress["processed_months"])

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
        consolidate_final()
        print()
        print("  COMPLETADO. Los archivos estan en:")
        print(f"  {PROCESSED_DIR}")
        print()
        if IS_INTERACTIVE:
            input("  Presiona ENTER para cerrar...")
        return

    start_time = time.time()
    months_processed_this_session = 0

    for tag in all_months:
        if interrupted:
            break

        if tag in processed_months:
            continue

        year, month = map(int, tag.split('-'))
        csv_path = RAW_DIR / f"lic_{tag}.csv"
        zip_path = RAW_DIR / f"lic_{tag}.zip"
        url = f"{BASE_URL}/{tag}.zip"

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

        # --- Descarga ---
        if not (csv_path.exists() and csv_path.stat().st_size > 1000):
            print("descargando...", end=" ", flush=True)

            if not download_file(url, zip_path):
                print("NO DISPONIBLE - skip")
                progress["processed_months"].append(tag)
                save_progress(progress)
                months_processed_this_session += 1
                continue

            size_mb = zip_path.stat().st_size / (1024 * 1024)
            print(f"({size_mb:.1f}MB)", end=" ", flush=True)

            extracted = extract_zip(zip_path, RAW_DIR)
            zip_path.unlink(missing_ok=True)

            if extracted is None:
                print("EXTRACTION FAILED - skip")
                progress["processed_months"].append(tag)
                save_progress(progress)
                months_processed_this_session += 1
                continue

            if extracted != csv_path:
                extracted.rename(csv_path)

        # --- Procesar ---
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        print(f"procesando ({csv_size:.0f}MB)...", end=" ", flush=True)

        df_cat, df_org, df_reg, df_prov, df_comp, records = process_csv(str(csv_path), year, month)

        if interrupted:
            csv_path.unlink(missing_ok=True)
            break

        if df_cat is not None and len(df_cat) > 0:
            save_monthly(df_cat, df_org, df_reg, df_prov, df_comp, tag)
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

        csv_path.unlink(missing_ok=True)

    # --- Resumen final ---
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
        consolidate_final()

    print()
    print(f"  Registros esta sesion: {total_licitaciones_session:,}")
    print(f"  Registros acumulados:  {progress['total_records']:,}")
    print(f"  Tiempo esta sesion:    {minutes}m {seconds}s")
    print(f"  Datos en:              {PROCESSED_DIR}")
    print("=" * 60)
    print()
    if IS_INTERACTIVE:
        input("  Presiona ENTER para cerrar...")


if __name__ == '__main__':
    main()
