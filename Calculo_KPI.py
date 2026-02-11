# app.py
# -*- coding: utf-8 -*-
"""
KPIs LDS (DATA) – Streamlit (NORMALIZA TEXTOS + BORRA ESPACIOS)
"""

import io
import re
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


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
# NORMALIZACIÓN / LIMPIEZA
# =========================
def _strip_and_fix_spaces(s: str) -> str:
    s = str(s).replace("\u00A0", " ")  # NBSP -> space
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_text(s) -> str:
    """
    Para filtrar incidencias (comparación robusta):
    - arregla espacios invisibles
    - lower
    - elimina tildes/diacríticos (Vehículo == Vehiculo)
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = _strip_and_fix_spaces(s).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def clean_text_for_output(s) -> str:
    """Para salida humana (detalle): no quita tildes, solo arregla espacios + trim."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return _strip_and_fix_spaces(s)


def clean_lp(s) -> str:
    """LP No. limpio para evitar #N/D por espacios invisibles."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return _strip_and_fix_spaces(s)


# =========================
# HELPERS
# =========================
def ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError("Faltan columnas requeridas:\n" + "\n".join(f"- {c}" for c in missing))


def add_weekday_filter(df: pd.DataFrame, col_fecha: str) -> pd.DataFrame:
    df = df.copy()
    df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
    return df[df[col_fecha].dt.weekday < 5]


def parse_distance_round(series: pd.Series) -> pd.Series:
    """
    Soporta coma o punto decimal.
    Ej: "807,2" -> 807.2 -> 807
    """
    s = series.copy()
    s = s.astype(str).str.replace("\u00A0", " ", regex=False).str.strip()
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").round(0)


def classify_range(dist_int: pd.Series):
    dentro = dist_int.notna() & (dist_int < 200)
    fuera = dist_int.isna() | (dist_int >= 200)
    return dentro, fuera


def safe_sheet_name(name: str) -> str:
    invalid = r'[:\\/?*\[\]]'
    name = re.sub(invalid, "_", name).strip()
    if not name:
        name = "Sheet"
    return name[:31]


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
    raise RuntimeError("No se pudo generar nombre único de hoja.")


def short_name(filename: str, max_len: int = 16) -> str:
    stem = Path(filename).stem
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem[:max_len] if len(stem) > max_len else stem


# =========================
# LÓGICA DE NEGOCIO
# =========================
def filter_by_incidencias(df: pd.DataFrame, incidencias: list[str], col_incid: str) -> pd.DataFrame:
    """
    No sobreescribe el texto original de incidencia.
    Crea _incid_norm para filtrar correctamente.
    """
    incid_set = {normalize_text(x) for x in incidencias}
    temp = df.copy()
    temp["_incid_norm"] = temp[col_incid].apply(normalize_text)
    return temp[temp["_incid_norm"].isin(incid_set)].copy()


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
        "LP No.": df_filtered[col_lp].apply(clean_lp),
        "Repartidor": df_filtered[col_rider].apply(clean_text_for_output),
        "Tiempo del Fracaso de la Entrega": pd.to_datetime(df_filtered[col_fecha], errors="coerce"),
        "Incidencia Marcada": df_filtered[col_incid].apply(clean_text_for_output),
        "Distancia de Marcaje": dist_int,
        "Rango": ["Dentro" if x else "Fuera" for x in dentro],
        "Categoría": categoria,
    })

    return detail.sort_values(["Tiempo del Fracaso de la Entrega", "Repartidor"], ascending=[True, True])


def process_one_df(df: pd.DataFrame) -> dict:
    COL_LP = "LP No."
    COL_RIDER = "Nombre del Repartidor"
    COL_FECHA = "Tiempo del Fracaso de la Entrega"
    COL_INCID = "Detalles de la Excepción"
    COL_DIST = "Distancia de brecha de entrega"

    ensure_columns(df, [COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST])

    # Limpieza preventiva (reduce #N/D)
    df = df.copy()
    df[COL_LP] = df[COL_LP].apply(clean_lp)
    df[COL_RIDER] = df[COL_RIDER].apply(clean_text_for_output)
    df[COL_INCID] = df[COL_INCID].apply(clean_text_for_output)

    df_wd = add_weekday_filter(df, COL_FECHA)

    general_summary = build_general_summary(df_wd, COL_DIST)

    fake_df = filter_by_incidencias(df_wd, FAKE_ATTEMPTS_INCIDENCIAS, COL_INCID)
    fake_summary = build_summary_by_rider(fake_df, COL_RIDER, parse_distance_round(fake_df[COL_DIST]))
    fake_detail = build_detail(fake_df, COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST, "Fake")

    fg_df = filter_by_incidencias(df_wd, FALSA_GESTION_INCIDENCIAS, COL_INCID)
    fg_summary = build_summary_by_rider(fg_df, COL_RIDER, parse_distance_round(fg_df[COL_DIST]))
    fg_detail = build_detail(fg_df, COL_LP, COL_RIDER, COL_FECHA, COL_INCID, COL_DIST, "Falsa_gestion")

    detail_all = pd.concat([fake_detail, fg_detail], ignore_index=True)

    return {
        "general_summary": general_summary,
        "fake_summary": fake_summary,
        "fg_summary": fg_summary,
        "detail": detail_all,
    }


def build_outputs(uploaded_files) -> tuple[bytes, str | None]:
    """
    Retorna (excel_bytes, error_txt_or_none)
    - Continúa si un archivo falla
    - Acumula logs en un TXT descargable
    """
    output = io.BytesIO()
    used_sheets: set[str] = set()
    error_logs: list[str] = []

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for uf in uploaded_files:
            name = getattr(uf, "name", "archivo")
            try:
                df = pd.read_excel(uf)
                data = process_one_df(df)

                short = short_name(name, max_len=16)
                sh_res = make_unique_sheet(f"Resumen_{short}", used_sheets)
                sh_fake = make_unique_sheet(f"Fake_{short}", used_sheets)
                sh_fg = make_unique_sheet(f"Falsa_gestion_{short}", used_sheets)
                sh_det = make_unique_sheet(f"Detalle_{short}", used_sheets)

                data["general_summary"].to_excel(writer, index=False, sheet_name=sh_res)
                data["fake_summary"].to_excel(writer, index=False, sheet_name=sh_fake)
                data["fg_summary"].to_excel(writer, index=False, sheet_name=sh_fg)
                data["detail"].to_excel(writer, index=False, sheet_name=sh_det)

            except Exception as e:
                tb = traceback.format_exc()
                error_logs.append(
                    f"===== ERROR ARCHIVO =====\n"
                    f"Archivo: {name}\n"
                    f"Fecha: {datetime.now()}\n"
                    f"Error: {e}\n\n"
                    f"--- TRACEBACK ---\n{tb}\n"
                )

    excel_bytes = output.getvalue()
    error_txt = "\n\n".join(error_logs) if error_logs else None
    return excel_bytes, error_txt


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="KPIs LDS - Fake / Falsa gestión", layout="wide")
st.title("KPIs LDS (DATA) – Fake Deliverys y Falsa gestión")
st.caption("Sube uno o varios Excel DATA. Se generará un Excel con hojas por archivo: Resumen, Fake, Falsa_gestion y Detalle. (Solo L–V, sin fines de semana).")

files = st.file_uploader(
    "Sube archivos Excel (.xlsx / .xls). Puedes seleccionar varios.",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

col1, col2 = st.columns([1, 2])
with col1:
    run_btn = st.button("Generar Excel", type="primary", disabled=not files)
with col2:
    st.info("En web no se sube carpeta directa: selecciona varios archivos a la vez (equivale a una carpeta).")

if run_btn and files:
    with st.spinner("Procesando archivos..."):
        excel_bytes, error_txt = build_outputs(files)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"RESULTADOS_KPIS_{ts}.xlsx"

    st.success("Listo ✅")
    st.download_button(
        label="⬇️ Descargar Excel de resultados",
        data=excel_bytes,
        file_name=out_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    if error_txt:
        st.warning("Algunos archivos fallaron. Descarga el log para ver cuál y por qué.")
        st.download_button(
            label="⬇️ Descargar LOG de errores (.txt)",
            data=error_txt.encode("utf-8"),
            file_name=f"ERROR_KPIS_LDS_{ts}.txt",
            mime="text/plain"
        )

    with st.expander("Vista rápida (primer archivo)"):
        try:
            df0 = pd.read_excel(files[0])
            data0 = process_one_df(df0)
            st.subheader("Resumen (general)")
            st.dataframe(data0["general_summary"], use_container_width=True)
            st.subheader("Fake (top 20)")
            st.dataframe(data0["fake_summary"].head(20), use_container_width=True)
            st.subheader("Falsa_gestion (top 20)")
            st.dataframe(data0["fg_summary"].head(20), use_container_width=True)
        except Exception:
            st.write("No se pudo generar vista rápida para el primer archivo.")
