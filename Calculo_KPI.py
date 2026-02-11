# -*- coding: utf-8 -*-
"""
CALCULO KPIs LDS - VERSION FINAL "EMPRESARIAL"
=============================================
✅ Soporta:
- Seleccionar ARCHIVO o CARPETA (ventana)
- O usar CLI: --input / --folder / --output
- Procesa todos los Excel (.xlsx/.xls) y genera 1 SOLO Excel de salida
- Por cada archivo crea hojas:
    Resumen_<archivo>        -> TOTAL GENERAL (sin filtro de incidencias) Dentro/Fuera (L-V)
    Fake_<archivo>           -> Resumen por repartidor + TOTAL GENERAL
    Falsa_gestion_<archivo>  -> Resumen por repartidor + TOTAL GENERAL
    Detalle_<archivo>        -> LP No., Repartidor, Fecha, Incidencia, Distancia redondeada, Rango, Categoría
- Filtra fines de semana (solo L-V) para TODOS los cálculos
- Distancia: redondea a entero (ej: 200.0 -> 200) antes de clasificar
- Fuera de rango incluye distancia vacía (NaN) (como tu Excel actual)
- Manejo de errores:
    - Si un archivo falla, se registra en TXT y el proceso continúa con los demás.
    - Si falla algo global, también genera TXT.

Requisitos:
  pip install pandas openpyxl

Uso:
  python Calculo_KPI_Martha.py
  python Calculo_KPI_Martha.py --folder "C:\\RUTA\\CARPETA"
  python Calculo_KPI_Martha.py --input  "C:\\RUTA\\archivo.xlsx" --output "C:\\RUTA\\SALIDA.xlsx"
"""

import argparse
import re
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd


# =========================
# CONFIG (tus listas)
# =========================
FAKE_ATTEMPTS_INCIDENCIAS = [
    "Cliente no Disponible",
    "Cambio de Fecha Solicitado por el Cliente",
    "Paquete Dañado",
    "PUDO -  Cerrado Temporalmente",
    "Fuera de Horario Comercial",
    "PUDO - Fuera de Horario Comercial",
    "PUDO - No trabaja con Ecoscooting",
    "Rechazado por el cliente",
]

FALSA_GESTION_INCIDENCIAS = [
    "Clima Adverso",
    "Fuerza Mayor",
    "Falta de Tiempo",
    "Rechazado por el Cliente",
    "Vehículo Averiado",
]


# =========================
# LOG DE ERRORES
# =========================
def write_error_log(base_dir: Path, error: Exception, file_context: str = "", extra: str = "") -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = base_dir / f"ERROR_KPIS_LDS_{ts}.txt"

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("===== ERROR KPIs LDS =====\n")
        f.write(f"Fecha: {datetime.now()}\n")
        if file_context:
            f.write(f"Archivo en proceso: {file_context}\n")
        if extra:
            f.write("\n--- INFO EXTRA ---\n")
            f.write(extra + "\n")

        f.write("\n--- ERROR ---\n")
        f.write(str(error) + "\n")

        f.write("\n--- TRACEBACK ---\n")
        f.write(traceback.format_exc())

    return log_path


# =========================
# HELPERS
# =========================
def normalize_text(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).replace("\u00A0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError("Faltan columnas requeridas:\n" + "\n".join(f"- {c}" for c in missing))


def add_weekday_filter(df: pd.DataFrame, col_fecha: str) -> pd.DataFrame:
    df = df.copy()
    df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
    # L-V (0-4). Si fecha es NaT, queda fuera por NaT.weekday = NaN (filtrado por comparación)
    return df[df[col_fecha].dt.weekday < 5]


def parse_distance_round(series: pd.Series) -> pd.Series:
    # Convierte a número y redondea a entero (0 decimales)
    return pd.to_numeric(series, errors="coerce").round(0)


def classify_range(dist_int: pd.Series) -> tuple[pd.Series, pd.Series]:
    # Dentro: <200 y no NaN
    # Fuera : >=200 o NaN
    dentro = dist_int.notna() & (dist_int < 200)
    fuera = dist_int.isna() | (dist_int >= 200)
    return dentro, fuera


def safe_sheet_name(name: str) -> str:
    # Excel max 31, y sin caracteres inválidos
    invalid = r'[:\\/?*\[\]]'
    name = re.sub(invalid, "_", name).strip()
    if not name:
        name = "Sheet"
    return name[:31]


def file_stem_short(path: Path, max_len: int = 16) -> str:
    # Corto para que entren prefijos + max 31
    stem = re.sub(r"\s+", " ", path.stem).strip()
    return stem[:max_len] if len(stem) > max_len else stem


def make_unique_sheet(base: str, used: set[str]) -> str:
    base = safe_sheet_name(base)
    if base not in used:
        used.add(base)
        return base
    for i in range(2, 1000):
        candidate = safe_sheet_name(f"{base[:27]}_{i}")
        if candidate not in used:
            used.add(candidate)
            return candidate
    raise RuntimeError("No se pudo generar un nombre único de hoja.")


def filter_by_incidencias(df: pd.DataFrame, incidencias: list[str], col_incid: str) -> pd.DataFrame:
    incid_set = {normalize_text(x) for x in incidencias}
    temp = df.copy()
    temp[col_incid] = temp[col_incid].apply(normalize_text)
    return temp[temp[col_incid].isin(incid_set)].copy()


def build_summary_by_rider(filtered: pd.DataFrame, rider_col: str, dist_int: pd.Series) -> pd.DataFrame:
    temp = filtered.copy()
    temp["_dist_int"] = dist_int
    dentro, fuera = classify_range(temp["_dist_int"])
    temp["_dentro"] = dentro
    temp["_fuera"] = fuera
    temp["__dummy__"] = 1

    grouped = temp.groupby(rider_col, dropna=False).agg(
        Total=("__dummy__", "size"),
        Dentro_de_rango=("_dentro", "sum"),
        Fuera_de_rango=("_fuera", "sum"),
        Sin_distancia=("_dist_int", lambda x: x.isna().sum()),
    )
    grouped = grouped.reset_index().rename(columns={rider_col: "Repartidor"})
    grouped = grouped.sort_values(["Total", "Repartidor"], ascending=[False, True])

    total_row = pd.DataFrame([{
        "Repartidor": "TOTAL GENERAL",
        "Total": int(grouped["Total"].sum()),
        "Dentro_de_rango": int(grouped["Dentro_de_rango"].sum()),
        "Fuera_de_rango": int(grouped["Fuera_de_rango"].sum()),
        "Sin_distancia": int(grouped["Sin_distancia"].sum()),
    }])

    return pd.concat([grouped, total_row], ignore_index=True)


def build_general_summary(df_weekdays: pd.DataFrame, col_dist: str) -> pd.DataFrame:
    dist_int = parse_distance_round(df_weekdays[col_dist])
    dentro, fuera = classify_range(dist_int)

    return pd.DataFrame([{
        "Total LP (L-V)": int(len(df_weekdays)),
        "LP Dentro de rango": int(dentro.sum()),
        "LP Fuera de rango": int(fuera.sum()),
        "LP Sin distancia": int(dist_int.isna().sum()),
    }])


def build_detail(df_filtered: pd.DataFrame,
                 col_lp: str,
                 col_rider: str,
                 col_fecha: str,
                 col_incid: str,
                 col_dist: str,
                 categoria: str) -> pd.DataFrame:
    dist_int = parse_distance_round(df_filtered[col_dist])
    dentro, _ = classify_range(dist_int)

    detail = pd.DataFrame({
        "LP No.": df_filtered[col_lp],
        "Repartidor": df_filtered[col_rider],
        "Tiempo del Fracaso de la Entrega": pd.to_datetime(df_filtered[col_fecha], errors="coerce"),
        "Incidencia Marcada": df_filtered[col_incid],
        "Distancia de Marcaje": dist_int,
        "Rango": ["Dentro" if x else "Fuera" for x in dentro],
        "Categoría": categoria,
    })

    return detail.sort_values(["Tiempo del Fracaso de la Entrega", "Repartidor"], ascending=[True, True])


# =========================
# CORE: PROCESAR 1 ARCHIVO
# =========================
def process_one_file(path: Path) -> dict:
    # Nombres exactos (según tu DATA)
    COL_LP = "LP No."
    COL_RIDER = "Nombre del Repartidor"
    COL_FECHA = "Tiempo del Fracaso de la Entrega"
    COL_INCID = "Detalles de la Excepción"
    COL_DIST = "Distancia de brecha de entrega"

    df = pd.read_excel(path, sheet_name=0)
    ensure_columns(df, [COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST])

    # Filtrar fines de semana para TODO
    df_wd = add_weekday_filter(df, COL_FECHA)

    # Resumen general (sin filtro incidencias)
    general_summary = build_general_summary(df_wd, COL_DIST)

    # Fake
    fake_df = filter_by_incidencias(df_wd, FAKE_ATTEMPTS_INCIDENCIAS, COL_INCID)
    fake_dist = parse_distance_round(fake_df[COL_DIST])
    fake_summary = build_summary_by_rider(fake_df, COL_RIDER, fake_dist)
    fake_detail = build_detail(fake_df, COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST, "Fake")

    # Falsa gestión
    fg_df = filter_by_incidencias(df_wd, FALSA_GESTION_INCIDENCIAS, COL_INCID)
    fg_dist = parse_distance_round(fg_df[COL_DIST])
    fg_summary = build_summary_by_rider(fg_df, COL_RIDER, fg_dist)
    fg_detail = build_detail(fg_df, COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST, "Falsa_gestion")

    # Detalle combinado (Fake + Falsa)
    detail_all = pd.concat([fake_detail, fg_detail], ignore_index=True)

    return {
        "general_summary": general_summary,
        "fake_summary": fake_summary,
        "fg_summary": fg_summary,
        "detail": detail_all,
    }


# =========================
# INPUT: LISTAR ARCHIVOS
# =========================
def list_excel_files(folder: Path) -> list[Path]:
    exts = {".xlsx", ".xls"}
    files = [
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in exts and not p.name.startswith("~$")
    ]
    return sorted(files, key=lambda p: p.name.lower())


# =========================
# GUI (Selector archivo/carpeta)
# =========================
def gui_choose_input() -> tuple[str | None, str | None]:
    """
    Devuelve (input_file, folder_path)
    - Primero intenta carpeta, si el usuario cancela, pide archivo.
    """
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    folder = filedialog.askdirectory(title="Selecciona la CARPETA con archivos DATA (Cancelar para elegir archivo)")
    if folder:
        return None, folder

    file_ = filedialog.askopenfilename(
        title="Selecciona un archivo DATA (Excel)",
        filetypes=[("Excel files", "*.xlsx *.xls")],
    )
    if file_:
        return file_, None

    return None, None


# =========================
# MAIN RUN
# =========================
def run(input_path: str | None, folder_path: str | None, output_path: str | None) -> None:
    base_dir = None
    out_path = None

    try:
        # 1) Resolver entrada
        if not input_path and not folder_path:
            input_path, folder_path = gui_choose_input()
            if not input_path and not folder_path:
                print("Proceso cancelado (no se seleccionó archivo ni carpeta).")
                return

        # 2) Listar archivos
        if folder_path:
            folder = Path(folder_path)
            if not folder.exists():
                raise ValueError(f"La carpeta no existe: {folder_path}")
            files = list_excel_files(folder)
            if not files:
                raise ValueError(f"No se encontraron Excel en la carpeta: {folder_path}")
            base_dir = folder
        else:
            file_ = Path(str(input_path))
            if not file_.exists():
                raise ValueError(f"El archivo no existe: {input_path}")
            files = [file_]
            base_dir = file_.parent

        # 3) Salida
        if output_path:
            out_path = Path(output_path)
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = base_dir / f"RESULTADOS_KPIS_{ts}.xlsx"

        used_sheet_names: set[str] = set()

        # 4) Procesar con tolerancia a fallos por archivo
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for f in files:
                try:
                    data = process_one_file(f)
                    short = file_stem_short(f, max_len=16)

                    sh_resumen = make_unique_sheet(f"Resumen_{short}", used_sheet_names)
                    sh_fake = make_unique_sheet(f"Fake_{short}", used_sheet_names)
                    sh_fg = make_unique_sheet(f"Falsa_gestion_{short}", used_sheet_names)
                    sh_det = make_unique_sheet(f"Detalle_{short}", used_sheet_names)

                    data["general_summary"].to_excel(writer, index=False, sheet_name=sh_resumen)
                    data["fake_summary"].to_excel(writer, index=False, sheet_name=sh_fake)
                    data["fg_summary"].to_excel(writer, index=False, sheet_name=sh_fg)
                    data["detail"].to_excel(writer, index=False, sheet_name=sh_det)

                    print(f"✅ Procesado: {f.name}")

                except Exception as e_file:
                    log_path = write_error_log(
                        base_dir=base_dir,
                        error=e_file,
                        file_context=str(f),
                        extra="Fallo por archivo. El proceso continúa con los demás."
                    )
                    print(f"❌ Falló: {f.name} | Log: {log_path.name}")

        print("\n✅ Proceso finalizado.")
        print(f"📄 Excel generado: {out_path}")

    except Exception as e:
        # error global
        safe_dir = base_dir if base_dir else Path.cwd()
        log_path = write_error_log(
            base_dir=safe_dir,
            error=e,
            file_context="(error global)",
            extra=f"Salida prevista: {out_path}" if out_path else ""
        )
        print(f"\n❌ Error global. Se generó log: {log_path}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KPIs LDS - Fake & Falsa gestión (empresarial)")
    parser.add_argument("--input", default=None, help="Ruta de un Excel DATA (LDS)")
    parser.add_argument("--folder", default=None, help="Ruta de carpeta con varios Excel DATA (LDS)")
    parser.add_argument("--output", default=None, help="Ruta del Excel final de salida")
    args = parser.parse_args()

    run(args.input, args.folder, args.output)