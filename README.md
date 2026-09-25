# vg-agroclima-datos

Proceso que fabrica los mapas de pronóstico de [VG Agroclima](https://agroclima.visiongeografica.com)
sin depender de la API de nadie: cuatro veces al día baja de los centros meteorológicos solo viento a 10 m,
temperatura a 2 m y precipitación de **ECMWF IFS**, **ECMWF AIFS** y **NOAA GFS** (0,25°), los convierte en
archivos binarios livianos y los publica en Cloudflare Pages (`https://vg-agroclima-datos.pages.dev`).

- `procesar.py` — descarga por rangos de bytes (solo las 4 variables), decodifica con ecCodes y escribe
  `manifest.json` + archivos por paso (globo a 0,5° y teselas de 30° a 0,25°) + precipitación diaria para la
  confianza del pronóstico. El formato está documentado al inicio del archivo.
- `.github/workflows/mapas.yml` — corre a las 02:30, 08:30, 14:30 y 20:30 UTC. El repositorio es público
  para que GitHub Actions no gaste cupo; no contiene secretos (la credencial de Cloudflare es un secreto
  cifrado del repositorio).
- Respaldo: el mismo guion puede correr en la Torre 1 si GitHub falla.

Licencias de los datos: ECMWF Open Data (CC BY 4.0, datos modificados) y NOAA GFS (dominio público).
