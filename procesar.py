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
def rangos_ecmwf(modelo, corrida, paso):
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
    quiero = {"10u": "u", "10v": "v", "2t": "t", "tp": "acc"}
    out = {}
    for linea in idx.text.splitlines():
        e = json.loads(linea)
        if e.get("param") in quiero and e.get("levtype") == "sfc":
            out[quiero[e["param"]]] = (base + ".grib2", e["_offset"], e["_offset"] + e["_length"] - 1)
    return out


def rangos_gfs(corrida, paso):
    f, h = corrida.strftime("%Y%m%d"), corrida.hour
    base = f"{S3_GFS}/gfs.{f}/{h:02d}/atmos/gfs.t{h:02d}z.pgrb2.0p25.f{paso:03d}"
    idx = S.get(base + ".idx", timeout=60)
    idx.raise_for_status()
    lineas = [l.split(":") for l in idx.text.strip().splitlines()]
    out = {}
    for i, l in enumerate(lineas):
        ini = int(l[1])
        fin = int(lineas[i + 1][1]) - 1 if i + 1 < len(lineas) else ""
        var, niv, cuando = l[3], l[4], l[5]
        clave = None
        if var == "UGRD" and niv == "10 m above ground":
            clave = "u"
        elif var == "VGRD" and niv == "10 m above ground":
            clave = "v"
        elif var == "TMP" and niv == "2 m above ground":
            clave = "t"
        elif var == "APCP" and niv == "surface" and cuando.startswith("0-"):
            clave = "acc"  # acumulado desde el inicio de la corrida ("0-9 hour", "0-7 day")
        if clave and clave not in out:
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
    """GRIB2 → matriz (721, 1440), norte arriba, longitudes desde -180."""
    h = eccodes.codes_new_from_message(buf)
    try:
        ni, nj = eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj")
        lo1 = eccodes.codes_get(h, "longitudeOfFirstGridPointInDegrees")
        jpos = eccodes.codes_get(h, "jScansPositively")
        unidad = eccodes.codes_get(h, "units")
        v = eccodes.codes_get_values(h).astype(np.float32).reshape(nj, ni)
    finally:
        eccodes.codes_release(h)
    if (ni, nj) != (NI, NJ):
        raise ValueError(f"malla inesperada {ni}x{nj}")
    if jpos:
        v = v[::-1]
    lo1 = ((lo1 + 180) % 360) - 180
    v = np.roll(v, int(round((lo1 + 180) / 0.25)) % NI, axis=1)
    return v * 1000.0 if unidad == "m" else v  # IFS da la precipitación en metros; AIFS y GFS, en kg/m² (= mm)


def bajar_paso(modelo, corrida, paso):
    m = MODELOS[modelo]
    rs = rangos_gfs(corrida, paso) if m["fuente"] == "gfs" else rangos_ecmwf(modelo, corrida, paso)
    out = {}
    for k in ("u", "v", "t", "acc"):
        if k not in rs:
            if k == "acc" and paso == 0:
                out[k] = np.zeros((NJ, NI), np.float32)
                continue
            raise RuntimeError(f"{modelo} {corrida:%Y%m%d%H} +{paso}h: falta {k}")
        out[k] = decodificar(bajar(*rs[k]))
    out["t"] -= 273.15
    return out


# ---------------------------------------------------------------- codificación
def q_lin(x):
    return np.clip(np.rint((x + 50.0) * 2.55), 0, 255).astype(np.uint8)


def q_raiz(x, tope):
    return np.clip(np.rint(np.sqrt(np.clip(x, 0, tope) / tope) * 255), 0, 255).astype(np.uint8)


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


def procesar(modelo, salida, max_pasos=None):
    m = MODELOS[modelo]
    t0 = time.time()
    corrida = ultima_corrida(modelo)
    pasos = m["pasos"][: max_pasos or None]
    tag = corrida.strftime("%Y%m%d%H")
    raiz = os.path.join(salida, modelo, tag)
    log(f"{modelo}: corrida {tag}Z, {len(pasos)} pasos")

    with ThreadPoolExecutor(8) as ex:
        campos = list(ex.map(lambda p: bajar_paso(modelo, corrida, p), pasos))
    log(f"{modelo}: descargado en {time.time() - t0:.0f} s")

    acc = np.stack([c["acc"] for c in campos])            # (pasos, 721, 1440) mm acumulados
    acc = np.maximum.accumulate(np.maximum(acc, 0), axis=0)  # nunca decrece (ruido de empaquetado)
    horas = np.array(pasos, np.float32)
    peso, n = 0, 0
    for k, p in enumerate(pasos):
        c = campos[k]
        kk = max(k, 1) if len(pasos) > 1 else 0
        tasa = (acc[kk] - acc[kk - 1]) / max(horas[kk] - horas[kk - 1], 1) if kk else np.zeros_like(acc[0])
        planos = [extender(q_lin(c["u"])), extender(q_lin(c["v"])), extender(q_lin(c["t"])), extender(q_raiz(tasa, 100))]
        peso += escribir(f"{raiz}/g/{p}.bin", empaquetar([x[::2, ::2] for x in planos])); n += 1
        for j, i, r0, c0 in teselas():
            peso += escribir(f"{raiz}/r/{j}_{i}/{p}.bin",
                             empaquetar([x[r0:r0 + CELDAS_R, c0:c0 + CELDAS_R] for x in planos])); n += 1
        if k == len(pasos) // 2:
            log(f"{modelo}: +{p}h  viento medio {np.hypot(c['u'], c['v']).mean():.1f} m/s · "
                f"temp media {c['t'].mean():.1f} °C · lluvia media {tasa.mean() * 24:.2f} mm/día")
        campos[k] = None

    # precipitación por día local de cada columna de teselas (huso = centro de la tesela / 15)
    def acum_en(h):
        if h <= horas[0]:
            return np.zeros_like(acc[0])
        if h >= horas[-1]:
            return acc[-1]
        k = int(np.searchsorted(horas, h)) - 1
        f = (h - horas[k]) / (horas[k + 1] - horas[k])
        return acc[k] * (1 - f) + acc[k + 1] * f

    for i in range(360 // TESELA):
        huso = round((-180 + TESELA * i + TESELA / 2) / 15)
        local = corrida + dt.timedelta(hours=huso)
        ini0 = (dt.datetime(local.year, local.month, local.day) - dt.timedelta(hours=huso) - corrida).total_seconds() / 3600
        dias = [acum_en(ini0 + 24 * (d + 1)) - acum_en(ini0 + 24 * d) for d in range(DIAS)]
        for j, _, r0, c0 in [t for t in teselas() if t[1] == i]:
            peso += escribir(f"{raiz}/d/{j}_{i}.bin",
                             empaquetar([extender(q_raiz(x, 400))[r0:r0 + CELDAS_R, c0:c0 + CELDAS_R] for x in dias])); n += 1

    log(f"{modelo}: {n} archivos, {peso / 1e6:.1f} MB, {time.time() - t0:.0f} s")
    return {"nombre": m["nombre"], "licencia": m["licencia"], "corrida": corrida.strftime("%Y-%m-%dT%H:00Z"),
            "ruta": f"{modelo}/{tag}", "pasos": pasos, "archivos": n, "mb": round(peso / 1e6, 1)}


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
