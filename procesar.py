#!/usr/bin/env python3
"""VG Agroclima · mapas propios de pronóstico.

Baja de los centros solo tres variables de superficie —viento a 10 m, temperatura a 2 m y
precipitación— de ECMWF IFS, ECMWF AIFS y NOAA GFS a 0,25°, y las convierte en archivos
binarios livianos que el mapa de Agroclima lee directamente. No usa secretos: todo es dato
abierto (ECMWF Open Data, CC BY 4.0; NOAA GFS, dominio público).

Salida (carpeta --salida):
  manifest.json                          qué corrida hay de cada modelo y cómo leerla
  {modelo}/{corrida}/g/{paso}.bin        todo el globo a 0,5° (un archivo por paso)
  {modelo}/{corrida}/r/{fila}_{col}/{paso}.bin   teselas de 30° a 0,25° (latitudes 60 N a 60 S)
  {modelo}/{corrida}/d/{fila}_{col}.bin  precipitación por día local, 9 días (para la confianza)

Cada .bin es zlib (DecompressionStream "deflate" en el navegador) de bytes sin signo, por
planos; cada fila va codificada como diferencias con el vecino de la izquierda (módulo 256),
que comprime mucho mejor en campos suaves. Planos de g y r: u, v, t, p.
  u, v  viento (m/s)            byte = (x + 50) * 2,55
  t     temperatura (°C)        byte = (x + 50) * 2,55
  p     precipitación (mm/h)    byte = sqrt(x / 100) * 255   (media del intervalo que termina en el paso)
  d     precipitación (mm/día)  byte = sqrt(x / 400) * 255
  x/    capas adicionales por paso, globo a 0,5°: presión (hPa), nubosidad (%), radiación solar (W/m²), humedad
        relativa (%), viento a 850 hPa y a 100 m (m/s), ráfagas (m/s), CAPE (J/kg), humedad del suelo (m³/m³) y
        lluvia acumulada desde el inicio de la corrida (mm). Codificación de cada plano en el manifiesto.
  a15.bin  lluvia acumulada a +1…+15 días (globo a 0,5°)
"""
import argparse
import datetime as dt
import json
import os
import sys
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import eccodes

# espejos de ECMWF Open Data, en orden (el de Amazon responde 503 a pedidos válidos: no se usa)
ECMWF_BASES = ["https://storage.googleapis.com/ecmwf-open-data", "https://data.ecmwf.int/forecasts"]
S3_GFS = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
PASOS_3_6 = list(range(0, 145, 3)) + list(range(150, 193, 6))  # 57 pasos, 8 días
PASOS_6 = list(range(0, 193, 6))                                 # 33 pasos, 8 días
MODELOS = {
    "ifs": {"nombre": "ECMWF IFS", "fuente": "ecmwf", "ruta": "ifs/0p25/oper", "ciclos": (0, 12), "pasos": PASOS_3_6,
            "licencia": "ECMWF Open Data · CC BY 4.0"},
    "aifs": {"nombre": "ECMWF AIFS", "fuente": "ecmwf", "ruta": "aifs-single/0p25/oper", "ciclos": (0, 6, 12, 18), "pasos": PASOS_6,
             "licencia": "ECMWF Open Data · CC BY 4.0"},
    "gfs": {"nombre": "NOAA GFS", "fuente": "gfs", "ciclos": (0, 6, 12, 18), "pasos": PASOS_3_6,
            "licencia": "NOAA · dominio público"},
}
NI, NJ = 1440, 721          # malla de 0,25°: lon -180..179,75 · lat 90..-90
FILAS_R = [60, 30, 0, -30]  # borde norte de cada fila de teselas
TESELA = 30                 # grados por tesela
CELDAS_R = TESELA * 4 + 1   # 121 (incluye el borde compartido)
DIAS = 9

S = requests.Session()
S.mount("https://", HTTPAdapter(max_retries=Retry(total=4, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504]), pool_maxsize=16))
S.headers["User-Agent"] = "vg-agroclima-datos (+https://agroclima.visiongeografica.com)"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- qué corrida hay
def existe(modelo, corrida, paso):
    m = MODELOS[modelo]
    f, h = corrida.strftime("%Y%m%d"), corrida.hour
    if m["fuente"] == "gfs":
        r = S.head(f"{S3_GFS}/gfs.{f}/{h:02d}/atmos/gfs.t{h:02d}z.pgrb2.0p25.f{paso:03d}.idx", timeout=30)
        return r.status_code == 200
    r = requests.head(f"{ECMWF_BASES[0]}/{f}/{h:02d}z/{m['ruta']}/{f}{h:02d}0000-{paso}h-oper-fc.index", timeout=30)
    return r.status_code == 200


def ultima_corrida(modelo):
    m = MODELOS[modelo]
    ahora = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    for k in range(0, 54):
        c = ahora - dt.timedelta(hours=k)
        if c.hour in m["ciclos"] and existe(modelo, c, m["pasos"][-1]):
            return c
    raise RuntimeError(f"{modelo}: no hay corrida completa en las últimas 54 horas")


# ---------------------------------------------------------------- descarga
# ---------------------------------------------------------------- qué se baja de cada fuente
# clave interna → (param, levtype, levelist) en el índice de ECMWF
ECMWF_CAMPOS = {
    "u": ("10u", "sfc", None), "v": ("10v", "sfc", None), "t": ("2t", "sfc", None), "acc": ("tp", "sfc", None),
    "msl": ("msl", "sfc", None), "tcc": ("tcc", "sfc", None), "ssrd": ("ssrd", "sfc", None), "d2": ("2d", "sfc", None),
    "u850": ("u", "pl", "850"), "v850": ("v", "pl", "850"), "u100": ("100u", "sfc", None), "v100": ("100v", "sfc", None),
    "gust": ("10fg", "sfc", None), "cape": ("mucape", "sfc", None), "suelo": ("vsw", "sol", "1"),
}
# clave interna → (variable, nivel, condición sobre el campo de tiempo) en el índice de GFS
GFS_CAMPOS = {
    "u": ("UGRD", "10 m above ground", None), "v": ("VGRD", "10 m above ground", None), "t": ("TMP", "2 m above ground", None),
    "acc": ("APCP", "surface", lambda c: c.startswith("0-")),  # acumulado desde el inicio ("0-9 hour", "0-7 day")
    "msl": ("PRMSL", "mean sea level", None), "tcc": ("TCDC", "entire atmosphere", lambda c: "ave" not in c),
    "rad": ("DSWRF", "surface", None), "rh": ("RH", "2 m above ground", None),
    "u850": ("UGRD", "850 mb", None), "v850": ("VGRD", "850 mb", None),
    "u100": ("UGRD", "100 m above ground", None), "v100": ("VGRD", "100 m above ground", None),
    "gust": ("GUST", "surface", None), "cape": ("CAPE", "surface", None), "suelo": ("SOILW", "0-0.1 m below ground", None),
}
BASE = ("u", "v", "t", "acc")


def rangos_ecmwf(modelo, corrida, paso, solo=None):
    m = MODELOS[modelo]
    f, h = corrida.strftime("%Y%m%d"), corrida.hour
    for espejo in ECMWF_BASES:
        base = f"{espejo}/{f}/{h:02d}z/{m['ruta']}/{f}{h:02d}0000-{paso}h-oper-fc"
        try:
            idx = S.get(base + ".index", timeout=60)
            idx.raise_for_status()
            break
        except requests.RequestException:
            if espejo == ECMWF_BASES[-1]:
                raise
    buscados = {v: k for k, v in ECMWF_CAMPOS.items() if solo is None or k in solo}
    out = {}
    for linea in idx.text.splitlines():
        e = json.loads(linea)
        clave = buscados.get((e.get("param"), e.get("levtype"), e.get("levelist")))
        if clave and clave not in out:
            out[clave] = (base + ".grib2", e["_offset"], e["_offset"] + e["_length"] - 1)
    return out


def rangos_gfs(corrida, paso, solo=None):
    f, h = corrida.strftime("%Y%m%d"), corrida.hour
    base = f"{S3_GFS}/gfs.{f}/{h:02d}/atmos/gfs.t{h:02d}z.pgrb2.0p25.f{paso:03d}"
    idx = S.get(base + ".idx", timeout=60)
    idx.raise_for_status()
    lineas = [l.split(":") for l in idx.text.strip().splitlines()]
    out = {}
    for i, l in enumerate(lineas):
        ini = int(l[1])
        fin = int(lineas[i + 1][1]) - 1 if i + 1 < len(lineas) else ""
        for clave, (var, niv, cond) in GFS_CAMPOS.items():
            if (solo is None or clave in solo) and clave not in out and l[3] == var and l[4] == niv and (cond is None or cond(l[5])):
                out[clave] = (base, ini, fin)
    return out


def bajar(url, ini, fin):
    r = S.get(url, headers={"Range": f"bytes={ini}-{fin}"}, timeout=120)
    r.raise_for_status()
    return r.content


CANDADO = threading.Lock()  # ecCodes no es seguro entre hilos (en Windows se cae): se decodifica de a uno


def decodificar(buf):
    with CANDADO:
        return _decodificar(buf)


def _decodificar(buf):
    """GRIB2 → (matriz (721, 1440) norte arriba y longitudes desde -180, unidad)."""
    h = eccodes.codes_new_from_message(buf)
    try:
        ni, nj = eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj")
        lo1 = eccodes.codes_get(h, "longitudeOfFirstGridPointInDegrees")
        jpos = eccodes.codes_get(h, "jScansPositively")
        unidad = eccodes.codes_get(h, "units")
        v = eccodes.codes_get_values(h).astype(np.float32).reshape(nj, ni)
        faltante = eccodes.codes_get(h, "missingValue")
    finally:
        eccodes.codes_release(h)
    if (ni, nj) != (NI, NJ):
        raise ValueError(f"malla inesperada {ni}x{nj}")
    v[v == faltante] = np.nan
    if jpos:
        v = v[::-1]
    lo1 = ((lo1 + 180) % 360) - 180
    return np.roll(v, int(round((lo1 + 180) / 0.25)) % NI, axis=1), unidad


def bajar_paso(modelo, corrida, paso, solo=None):
    m = MODELOS[modelo]
    rs = rangos_gfs(corrida, paso, solo) if m["fuente"] == "gfs" else rangos_ecmwf(modelo, corrida, paso, solo)
    out = {}
    for k in [x for x in BASE if solo is None or x in solo]:  # las capas extra pueden faltar (p. ej. ráfagas en AIFS)
        if k not in rs:
            if k == "acc" and paso == 0:
                out[k] = np.zeros((NJ, NI), np.float32)
                continue
            raise RuntimeError(f"{modelo} {corrida:%Y%m%d%H} +{paso}h: falta {k}")
    for k, rango in rs.items():
        v, unidad = decodificar(bajar(*rango))
        if k == "acc" and unidad == "m":
            v = v * 1000.0  # IFS da la precipitación en metros; AIFS y GFS, en kg/m² (= mm)
        out[k] = v
    out["t"] = out["t"] - 273.15 if "t" in out else None
    return out


# ---------------------------------------------------------------- codificación
def q_lin(x):
    return np.clip(np.rint(np.nan_to_num(x + 50.0) * 2.55), 0, 255).astype(np.uint8)


def q_raiz(x, tope):
    return np.clip(np.rint(np.sqrt(np.clip(np.nan_to_num(x), 0, tope) / tope) * 255), 0, 255).astype(np.uint8)


def q_rango(x, a, b):
    """lineal de a..b a 0..255"""
    return np.clip(np.rint((np.nan_to_num(x, nan=a) - a) / (b - a) * 255), 0, 255).astype(np.uint8)


def empaquetar(planos):
    a = np.ascontiguousarray(np.stack(planos))
    d = a.copy()
    d[..., 1:] = a[..., 1:] - a[..., :-1]  # uint8: la resta envuelve módulo 256
    return zlib.compress(d.tobytes(), 9)


def extender(v):
    """Agrega la columna de lon 180 (= -180) para interpolar sobre el antimeridiano."""
    return np.concatenate([v, v[:, :1]], axis=1)


def escribir(ruta, datos):
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "wb") as fh:
        fh.write(datos)
    return len(datos)


def teselas():
    for j, norte in enumerate(FILAS_R):
        for i in range(360 // TESELA):
            yield j, i, (90 - norte) * 4, i * TESELA * 4


# planos del archivo x/ (capas adicionales, globo a 0,5°): nombre, codificación
EXTRA = ["msl", "tcc", "rad", "rh", "u850", "v850", "u100", "v100", "gust", "cape", "suelo", "pacc"]
EXTRA_COD = {"msl": ["lin", 950, 1077.5], "tcc": ["lin", 0, 100], "rad": ["lin", 0, 1200], "rh": ["lin", 0, 100],
             "u850": ["lin", -50, 50], "v850": ["lin", -50, 50], "u100": ["lin", -50, 50], "v100": ["lin", -50, 50],
             "gust": ["lin", 0, 60], "cape": ["raiz", 5000], "suelo": ["lin", 0, 0.6], "pacc": ["raiz", 1000]}
EXTRA_TODO = [k for k in ECMWF_CAMPOS if k not in BASE] + ["rad", "rh"]


def humedad(t, d):
    return np.clip(100 * np.exp(17.625 * d / (243.04 + d)) / np.exp(17.625 * t / (243.04 + t)), 0, 100)


def planos_extra(c, c_prev, dt_h, pacc):
    """Convierte los campos crudos de un paso en los planos de EXTRA (y dice cuáles había)."""
    hay, P = [], {}
    if c.get("msl") is not None: P["msl"] = c["msl"] / 100.0                       # Pa → hPa
    if c.get("tcc") is not None:
        x = c["tcc"]; P["tcc"] = x * 100.0 if np.nanmax(x) <= 1.01 else x           # fracción o %
    if c.get("rad") is not None: P["rad"] = c["rad"]                                # GFS: W/m² medio del intervalo
    elif c.get("ssrd") is not None and c_prev is not None and c_prev.get("ssrd") is not None and dt_h > 0:
        P["rad"] = np.maximum(c["ssrd"] - c_prev["ssrd"], 0) / (dt_h * 3600.0)       # J/m² acumulado → W/m²
    if c.get("rh") is not None: P["rh"] = c["rh"]
    elif c.get("d2") is not None and c.get("t") is not None: P["rh"] = humedad(c["t"], c["d2"] - 273.15)
    for k in ("u850", "v850", "u100", "v100", "gust", "cape", "suelo"):
        if c.get(k) is not None: P[k] = c[k]
    P["pacc"] = pacc
    planos = []
    for k in EXTRA:
        cod = EXTRA_COD[k]
        if k in P:
            hay.append(k)
            planos.append(extender(q_rango(P[k], cod[1], cod[2]) if cod[0] == "lin" else q_raiz(P[k], cod[1])))
        else:
            planos.append(np.zeros((NJ, NI + 1), np.uint8))
    return planos, hay


def procesar(modelo, salida, max_pasos=None):
    m = MODELOS[modelo]
    t0 = time.time()
    corrida = ultima_corrida(modelo)
    pasos = m["pasos"][: max_pasos or None]
    tag = corrida.strftime("%Y%m%d%H")
    raiz = os.path.join(salida, modelo, tag)
    log(f"{modelo}: corrida {tag}Z, {len(pasos)} pasos")
    horas = np.array(pasos, np.float32)
    acc_list, peso, n, hay_extra = [], 0, 0, set()
    ultimo = None  # campos del paso anterior (para diferencias de acumulados)
    todo = list(BASE) + EXTRA_TODO

    # por bloques de 8 pasos: se baja en paralelo y se procesa en orden, sin guardar toda la corrida en memoria
    for i0 in range(0, len(pasos), 8):
        lote = pasos[i0:i0 + 8]
        with ThreadPoolExecutor(8) as ex:
            campos = list(ex.map(lambda p: bajar_paso(modelo, corrida, p, [k for k in todo if k in (GFS_CAMPOS if m["fuente"] == "gfs" else ECMWF_CAMPOS) or k in ("rad", "rh")]), lote))
        # el paso 0 no tiene lluvia ni radiación acumulada: toma la tasa del paso 1
        for j, p in enumerate(lote):
            k = i0 + j
            c = campos[j]
            a = np.maximum(np.nan_to_num(c["acc"]), 0)
            if acc_list: a = np.maximum(a, acc_list[-1])  # nunca decrece (ruido de empaquetado)
            acc_list.append(a)
            if k == 0:
                sig = campos[j + 1] if len(lote) > 1 else None
                if sig is not None:
                    a1 = np.maximum(np.nan_to_num(sig["acc"]), a)
                    tasa = (a1 - a) / max(horas[1] - horas[0], 1)
                    planos_x, hay = planos_extra(sig, c, horas[1] - horas[0], a)
                else:
                    tasa = np.zeros_like(a); planos_x, hay = planos_extra(c, None, 0, a)
                # los campos instantáneos (presión, nubes, viento) sí son del paso 0
                inst, _ = planos_extra({kk: c.get(kk) for kk in c if kk not in ("ssrd", "rad")}, None, 0, a)
                for q, nombre in enumerate(EXTRA):
                    if nombre != "rad": planos_x[q] = inst[q]
            else:
                dt_h = max(horas[k] - horas[k - 1], 1)
                tasa = (a - acc_list[k - 1]) / dt_h
                planos_x, hay = planos_extra(c, ultimo, dt_h, a)
            hay_extra.update(hay)
            planos = [extender(q_lin(c["u"])), extender(q_lin(c["v"])), extender(q_lin(c["t"])), extender(q_raiz(tasa, 100))]
            peso += escribir(f"{raiz}/g/{p}.bin", empaquetar([x[::2, ::2] for x in planos])); n += 1
            peso += escribir(f"{raiz}/x/{p}.bin", empaquetar([x[::2, ::2] for x in planos_x])); n += 1
            for jj, ii, r0, c0 in teselas():
                peso += escribir(f"{raiz}/r/{jj}_{ii}/{p}.bin",
                                 empaquetar([x[r0:r0 + CELDAS_R, c0:c0 + CELDAS_R] for x in planos])); n += 1
            if k == len(pasos) // 2:
                log(f"{modelo}: +{p}h  viento medio {np.nanmean(np.hypot(c['u'], c['v'])):.1f} m/s · "
                    f"temp media {np.nanmean(c['t']):.1f} °C · lluvia media {np.nanmean(tasa) * 24:.2f} mm/día · capas extra: {','.join(sorted(hay))}")
            ultimo = {kk: c.get(kk) for kk in ("ssrd",)}
        del campos
    log(f"{modelo}: descargado y procesado en {time.time() - t0:.0f} s")
    acc = np.stack(acc_list)

    # precipitación por día local de cada columna de teselas (huso = centro de la tesela / 15)
    def acum_en(h, A=acc, H=horas):
        if h <= H[0]:
            return np.zeros_like(A[0])
        if h >= H[-1]:
            return A[-1]
        k = int(np.searchsorted(H, h)) - 1
        f = (h - H[k]) / (H[k + 1] - H[k])
        return A[k] * (1 - f) + A[k + 1] * f

    for i in range(360 // TESELA):
        huso = round((-180 + TESELA * i + TESELA / 2) / 15)
        local = corrida + dt.timedelta(hours=huso)
        ini0 = (dt.datetime(local.year, local.month, local.day) - dt.timedelta(hours=huso) - corrida).total_seconds() / 3600
        dias = [acum_en(ini0 + 24 * (d + 1)) - acum_en(ini0 + 24 * d) for d in range(DIAS)]
        for j, _, r0, c0 in [t for t in teselas() if t[1] == i]:
            peso += escribir(f"{raiz}/d/{j}_{i}.bin",
                             empaquetar([extender(q_raiz(x, 400))[r0:r0 + CELDAS_R, c0:c0 + CELDAS_R] for x in dias])); n += 1

    # lluvia acumulada a 15 días (globo a 0,5°): 15 planos, total desde el inicio de la corrida a +24 h, +48 h… +360 h
    a15 = None
    if not max_pasos:
        try:
            ext = [p for p in range(int(horas[-1]) + 6, 361, 6)]
            with ThreadPoolExecutor(8) as ex:
                mas = list(ex.map(lambda p: bajar_paso(modelo, corrida, p, ["acc"])["acc"], ext))
            A2 = [acc_list[-1]]
            for x in mas: A2.append(np.maximum(np.nan_to_num(x), A2[-1]))
            A15 = np.stack(acc_list + A2[1:]); H15 = np.array(list(pasos) + ext, np.float32)
            planos15 = [extender(q_raiz(acum_en(24 * d, A15, H15), 1000))[::2, ::2] for d in range(1, 16)]
            peso += escribir(f"{raiz}/a15.bin", empaquetar(planos15)); n += 1
            a15 = {"dias": 15, "codigo": ["raiz", 1000]}
            log(f"{modelo}: lluvia a 15 días lista ({len(ext)} pasos más)")
        except Exception as e:
            log(f"{modelo}: sin lluvia a 15 días: {e}")

    log(f"{modelo}: {n} archivos, {peso / 1e6:.1f} MB, {time.time() - t0:.0f} s")
    return {"nombre": m["nombre"], "licencia": m["licencia"], "corrida": corrida.strftime("%Y-%m-%dT%H:00Z"),
            "ruta": f"{modelo}/{tag}", "pasos": pasos, "archivos": n, "mb": round(peso / 1e6, 1),
            "extra": [k for k in EXTRA if k in hay_extra], "a15": a15}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default="sitio")
    ap.add_argument("--modelos", default="ifs,aifs,gfs")
    ap.add_argument("--max-pasos", type=int, default=None, help="solo para pruebas")
    a = ap.parse_args()
    man = {
        "version": 1,
        "generado": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "g": {"res": 0.5, "nx": 721, "ny": 361, "oeste": -180, "norte": 90},
        "r": {"res": 0.25, "tesela": TESELA, "celdas": CELDAS_R, "filas": FILAS_R, "columnas": 360 // TESELA},
        "d": {"dias": DIAS, "huso": "round((-180 + 30*col + 15) / 15) horas"},
        "x": {"res": 0.5, "planos": EXTRA, "codigo": EXTRA_COD, "nota": "capas adicionales por paso en {ruta}/x/{paso}.bin; a15.bin: lluvia acumulada a +1…+15 días"},
        "codigo": {"u": [-50, 50], "v": [-50, 50], "t": [-50, 50], "p": ["raiz", 100], "d": ["raiz", 400],
                   "planos": ["u", "v", "t", "p"], "filas": "diferencia con la izquierda, módulo 256", "compresion": "zlib"},
        "modelos": {},
    }
    fallos = []
    for k in a.modelos.split(","):
        try:
            man["modelos"][k] = procesar(k, a.salida, a.max_pasos)
        except Exception as e:  # un modelo caído no tumba a los otros dos
            log(f"{k}: FALLÓ: {e}")
            fallos.append(k)
    if not man["modelos"]:
        sys.exit("Ningún modelo se pudo procesar")
    with open(os.path.join(a.salida, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(man, fh, ensure_ascii=False, separators=(",", ":"))
    log("manifest listo;", "fallaron: " + ",".join(fallos) if fallos else "los tres modelos al día")


if __name__ == "__main__":
    main()
