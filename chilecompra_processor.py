#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================
  ChileCompra Data Processor - Windows Edition
  Descarga y procesamiento de licitaciones 2015-2026
  Autor: Guillermo Fuentes (djwillichile)
==============================================================

Descarga datos de licitaciones desde Azure Blob Storage (ChileCompra),
los procesa en streaming (chunks) y genera archivos Parquet listos
para analisis y visualizacion.

Uso:
    python chilecompra_processor.py

Requisitos:
    pip install pandas pyarrow requests
"""

import os
import sys
import zipfile
import requests
import pandas as pd
import json
import time
from pathlib import Path

# ==========================================
# CONFIGURACION
# ==========================================
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = BASE_DIR / "data" / "raw" / "licitaciones"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
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

# Acumuladores globales
agg_mensual_categoria = []
agg_anual_organismo = []
agg_anual_region = []
agg_anual_proveedor = []
agg_competencia = []
total_licitaciones = 0
total_files = 0


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
                print(f"  Reintentando ({attempt+1}/{max_retries})...", end=" ", flush=True)
                time.sleep(5)
            else:
                print(f"  ERROR: {e}")
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
        print("  [ZIP corrupto]", end=" ", flush=True)
    return None


def process_csv(csv_path, year, month):
    """Procesa un CSV en chunks y devuelve DataFrames agregados."""
    global total_licitaciones

    chunks_cat = []
    chunks_org = []
    chunks_reg = []
    chunks_prov = []
    chunks_comp = []

    try:
        for chunk in pd.read_csv(
            csv_path, sep=';', encoding='latin-1',
            usecols=USECOLS, dtype=str,
            chunksize=100000, on_bad_lines='skip', quoting=1
        ):
            # Convertir montos a numerico
            chunk['MontoLineaAdjudica'] = pd.to_numeric(chunk['MontoLineaAdjudica'], errors='coerce').fillna(0)
            chunk['MontoEstimado'] = pd.to_numeric(chunk['MontoEstimado'], errors='coerce').fillna(0)
            chunk['NumeroOferentes'] = pd.to_numeric(chunk['NumeroOferentes'], errors='coerce').fillna(0)

            # Monto efectivo: adjudicado si existe, sino estimado
            chunk['monto'] = chunk['MontoLineaAdjudica'].where(
                chunk['MontoLineaAdjudica'] > 0, chunk['MontoEstimado']
            )

            total_licitaciones += len(chunk)

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
        return None, None, None, None, None

    df_cat = pd.concat(chunks_cat, ignore_index=True) if chunks_cat else pd.DataFrame()
    df_org = pd.concat(chunks_org, ignore_index=True) if chunks_org else pd.DataFrame()
    df_reg = pd.concat(chunks_reg, ignore_index=True) if chunks_reg else pd.DataFrame()
    df_prov = pd.concat(chunks_prov, ignore_index=True) if chunks_prov else pd.DataFrame()
    df_comp = pd.concat(chunks_comp, ignore_index=True) if chunks_comp else pd.DataFrame()

    return df_cat, df_org, df_reg, df_prov, df_comp


def main():
    global total_files, total_licitaciones
    global agg_mensual_categoria, agg_anual_organismo, agg_anual_region
    global agg_anual_proveedor, agg_competencia

    print("=" * 60)
    print("  ChileCompra Data Processor")
    print("  Descarga y procesamiento de licitaciones 2015-2026")
    print("=" * 60)
    print(f"  Directorio de trabajo: {BASE_DIR}")
    print(f"  Datos procesados: {PROCESSED_DIR}")
    print("=" * 60)
    print()

    start_time = time.time()

    for year in range(START_YEAR, END_YEAR + 1):
        max_month = 4 if year == 2026 else 12
        for month in range(1, max_month + 1):
            tag = f"{year}-{month}"
            csv_path = RAW_DIR / f"lic_{tag}.csv"
            zip_path = RAW_DIR / f"lic_{tag}.zip"
            url = f"{BASE_URL}/{tag}.zip"

            print(f"[{tag:>7}] ", end="", flush=True)

            # --- Descargar ---
            if not (csv_path.exists() and csv_path.stat().st_size > 1000):
                print("Descargando...", end=" ", flush=True)

                if not download_file(url, zip_path):
                    print("NO DISPONIBLE - SKIP")
                    continue

                size_mb = zip_path.stat().st_size / (1024 * 1024)
                print(f"({size_mb:.1f} MB)", end=" ", flush=True)

                # --- Extraer ---
                extracted = extract_zip(zip_path, RAW_DIR)
                zip_path.unlink(missing_ok=True)

                if extracted is None:
                    print("EXTRACTION FAILED - SKIP")
                    continue

                # Renombrar al nombre estandar
                if extracted != csv_path:
                    extracted.rename(csv_path)

            # --- Procesar ---
            csv_size = csv_path.stat().st_size / (1024 * 1024)
            print(f"Procesando ({csv_size:.0f} MB)...", end=" ", flush=True)

            df_cat, df_org, df_reg, df_prov, df_comp = process_csv(str(csv_path), year, month)

            if df_cat is not None and len(df_cat) > 0:
                agg_mensual_categoria.append(df_cat)
                agg_anual_organismo.append(df_org)
                agg_anual_region.append(df_reg)
                agg_anual_proveedor.append(df_prov)
                agg_competencia.append(df_comp)
                total_files += 1
                print("OK")
            else:
                print("EMPTY/ERROR")

            # Eliminar CSV para ahorrar espacio
            csv_path.unlink(missing_ok=True)

    # ==========================================
    # CONSOLIDAR Y GUARDAR
    # ==========================================
    print()
    print("=" * 60)
    print(f"  CONSOLIDANDO {total_files} archivos procesados...")
    print(f"  Total registros procesados: {total_licitaciones:,}")
    print("=" * 60)

    # 1. Agregado mensual por categoria
    if agg_mensual_categoria:
        df_all_cat = pd.concat(agg_mensual_categoria, ignore_index=True)
        df_cat_final = df_all_cat.groupby(['anio', 'mes', 'Rubro1']).agg(
            n_licitaciones=('n_licitaciones', 'sum'),
            monto_adjudicado=('monto_adjudicado', 'sum'),
            oferentes_promedio=('oferentes_promedio', 'mean'),
            n_proveedores=('n_proveedores', 'sum'),
            n_organismos=('n_organismos', 'sum')
        ).reset_index()
        out = PROCESSED_DIR / 'agregado_mensual_categoria.parquet'
        df_cat_final.to_parquet(out, index=False)
        print(f"  [OK] {out.name}: {len(df_cat_final):,} filas")

    # 2. Agregado anual por organismo
    if agg_anual_organismo:
        df_all_org = pd.concat(agg_anual_organismo, ignore_index=True)
        df_org_final = df_all_org.groupby(['anio', 'NombreOrganismo', 'sector']).agg(
            n_licitaciones=('n_licitaciones', 'sum'),
            monto_total=('monto_total', 'sum'),
            oferentes_promedio=('oferentes_promedio', 'mean')
        ).reset_index()
        out = PROCESSED_DIR / 'agregado_anual_organismo_clean.parquet'
        df_org_final.to_parquet(out, index=False)
        print(f"  [OK] {out.name}: {len(df_org_final):,} filas")

    # 3. Agregado anual por region
    if agg_anual_region:
        df_all_reg = pd.concat(agg_anual_region, ignore_index=True)
        df_reg_final = df_all_reg.groupby(['anio', 'RegionUnidad']).agg(
            n_licitaciones=('n_licitaciones', 'sum'),
            monto_total=('monto_total', 'sum'),
            n_organismos=('n_organismos', 'sum')
        ).reset_index()
        out = PROCESSED_DIR / 'agregado_anual_region_clean.parquet'
        df_reg_final.to_parquet(out, index=False)
        print(f"  [OK] {out.name}: {len(df_reg_final):,} filas")

    # 4. Top 500 proveedores por anio
    if agg_anual_proveedor:
        df_all_prov = pd.concat(agg_anual_proveedor, ignore_index=True)
        df_prov_agg = df_all_prov.groupby(['anio', 'NombreProveedor']).agg(
            n_adjudicaciones=('n_adjudicaciones', 'sum'),
            monto_total=('monto_total', 'sum'),
            n_rubros=('n_rubros', 'max')
        ).reset_index()
        top_provs = df_prov_agg.groupby('NombreProveedor')['monto_total'].sum().nlargest(500).index
        df_prov_final = df_prov_agg[df_prov_agg['NombreProveedor'].isin(top_provs)]
        out = PROCESSED_DIR / 'agregado_anual_proveedor_top500.parquet'
        df_prov_final.to_parquet(out, index=False)
        print(f"  [OK] {out.name}: {len(df_prov_final):,} filas")

    # 5. Competencia por rubro
    if agg_competencia:
        df_all_comp = pd.concat(agg_competencia, ignore_index=True)
        df_comp_final = df_all_comp.groupby(['anio', 'Rubro1']).agg(
            oferentes_promedio=('oferentes_promedio', 'mean'),
            oferentes_mediana=('oferentes_mediana', 'median'),
            n_licitaciones=('n_licitaciones', 'sum')
        ).reset_index()
        out = PROCESSED_DIR / 'competencia_por_rubro.parquet'
        df_comp_final.to_parquet(out, index=False)
        print(f"  [OK] {out.name}: {len(df_comp_final):,} filas")

    # 6. Metricas anuales por categoria (con crecimiento e indice de oportunidad)
    if agg_mensual_categoria:
        df_anual_cat = df_cat_final.groupby(['anio', 'Rubro1']).agg(
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
        print(f"  [OK] {out.name}: {len(df_anual_cat):,} filas")

    # 7. Sankey data 2024
    sankey_data_2024 = {'organismos': [], 'categorias': [], 'proveedores': [], 'regiones': []}
    if agg_anual_organismo and agg_mensual_categoria and agg_anual_proveedor and agg_anual_region:
        org_2024 = df_org_final[df_org_final['anio'] == 2024].groupby('NombreOrganismo')['monto_total'].sum().nlargest(15)
        sankey_data_2024['organismos'] = [{'NombreOrganismo': k, 'monto_total': float(v)} for k, v in org_2024.items()]

        cat_2024 = df_cat_final[df_cat_final['anio'] == 2024].groupby('Rubro1')['monto_adjudicado'].sum().nlargest(15)
        sankey_data_2024['categorias'] = [{'Rubro1': k, 'monto_adjudicado': float(v)} for k, v in cat_2024.items()]

        prov_2024 = df_prov_agg[df_prov_agg['anio'] == 2024].groupby('NombreProveedor')['monto_total'].sum().nlargest(15)
        sankey_data_2024['proveedores'] = [{'NombreProveedor': k, 'monto_total': float(v)} for k, v in prov_2024.items()]

        reg_2024 = df_reg_final[df_reg_final['anio'] == 2024].groupby('RegionUnidad')['monto_total'].sum().nlargest(15)
        sankey_data_2024['regiones'] = [{'RegionUnidad': k, 'monto_total': float(v)} for k, v in reg_2024.items()]

        out = PROCESSED_DIR / 'sankey_tops_2024.json'
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(sankey_data_2024, f, ensure_ascii=False, indent=2)
        print(f"  [OK] {out.name}: saved")

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print()
    print("=" * 60)
    print(f"  PROCESAMIENTO COMPLETO")
    print(f"  Archivos procesados: {total_files}")
    print(f"  Total registros: {total_licitaciones:,}")
    print(f"  Tiempo total: {minutes}m {seconds}s")
    print(f"  Parquets en: {PROCESSED_DIR}")
    print("=" * 60)
    print()
    input("Presiona ENTER para cerrar...")


if __name__ == '__main__':
    main()
