/* ============================================================
   PROYECTO: Limpieza y Estandarización de Datos en SQL
   DATASET: Reservas de agencia de viajes (CSV sucio ~18K)
   MOTOR: MySQL
   OBJETIVO: Generar tabla lista para reporte/KPIs preservando el raw
   AUTOR: (Tu nombre)
   ============================================================ */

-- ============================================================
-- 01) CONTEXTO / SETUP
-- ============================================================
USE portfolio;

-- Tamaño inicial del dataset (evidencia)
SELECT COUNT(*) AS total_reservas_sucias
FROM reservas_sucias;

-- ============================================================
-- 02) CREAR COPIA DEL DATASET ORIGINAL (TRAZABILIDAD)
--     - Se preserva reservas_sucias como raw.
--     - reservas_limpia es la tabla de trabajo.
-- ============================================================
CREATE TABLE reservas_limpia AS
SELECT * FROM reservas_sucias;

-- Verificar que la copia se creó correctamente
SELECT COUNT(*) AS total_reservas_limpia_inicial
FROM reservas_limpia;

-- ============================================================
-- 03) CORRECCIÓN DE BOM/ENCABEZADO (IMPORTACIÓN CSV)
--     - Algunos CSV traen BOM y la columna queda como `ï»¿reserva_id`.
-- ============================================================
ALTER TABLE reservas_limpia
CHANGE COLUMN `ï»¿reserva_id` ID_reserva VARCHAR(20) NULL;

-- Evidencia de estructura (opcional)
SHOW CREATE TABLE reservas_sucias;

-- ============================================================
-- 04) DEMO: IDENTIFICACIÓN DE VALORES FALTANTES (EJEMPLO)
--     - Muestra cómo NULLIF + TRIM transforma vacío -> NULL.
-- ============================================================
SELECT
  email AS Original,
  NULLIF(TRIM(email), '') AS despues_limpieza
FROM reservas_sucias;

-- ============================================================
-- 05) NORMALIZACIÓN DE CODIFICACIÓN (UTF8/LATIN1)
--     - Se aplica cuando la importación rompe acentos/ñ.
--     - Nota: esto es útil si detectaste caracteres corruptos.
-- ============================================================
UPDATE reservas_limpia
SET
  nombre_cliente = CONVERT(BINARY CONVERT(nombre_cliente USING latin1) USING utf8mb4),
  ID_reserva     = CONVERT(BINARY CONVERT(ID_reserva USING latin1) USING utf8mb4),
  cliente_id     = CONVERT(BINARY CONVERT(cliente_id USING latin1) USING utf8mb4),
  email          = CONVERT(BINARY CONVERT(email USING latin1) USING utf8mb4),
  destino        = CONVERT(BINARY CONVERT(destino USING latin1) USING utf8mb4),
  pais           = CONVERT(BINARY CONVERT(pais USING latin1) USING utf8mb4),
  canal_venta    = CONVERT(BINARY CONVERT(canal_venta USING latin1) USING utf8mb4),
  tipo_paquete   = CONVERT(BINARY CONVERT(tipo_paquete USING latin1) USING utf8mb4),
  estado_reserva = CONVERT(BINARY CONVERT(estado_reserva USING latin1) USING utf8mb4);

-- ============================================================
-- 06) NULOS DISFRAZADOS / VACÍOS -> NULL
--     - Se normalizan strings vacíos y tokens N/A, na, s/d.
-- ============================================================

-- Revisión rápida de valores (opcional)
SELECT DISTINCT LOWER(TRIM(motivo_cancelacion)) AS motivo_normalizado
FROM reservas_limpia
ORDER BY 1;

-- 06.1) Vacíos a NULL (NULLIF + TRIM)
UPDATE reservas_limpia
SET
  email             = NULLIF(TRIM(email), ''),
  telefono          = NULLIF(TRIM(telefono), ''),
  destino           = NULLIF(TRIM(destino), ''),
  pais              = NULLIF(TRIM(pais), ''),
  canal_venta       = NULLIF(TRIM(canal_venta), ''),
  tipo_paquete      = NULLIF(TRIM(tipo_paquete), ''),
  fecha_reserva     = NULLIF(TRIM(fecha_reserva), ''),
  fecha_viaje       = NULLIF(TRIM(fecha_viaje), ''),
  fecha_cancelacion = NULLIF(TRIM(fecha_cancelacion), ''),
  motivo_cancelacion= NULLIF(TRIM(motivo_cancelacion), '');

-- 06.2) Tokens a NULL
UPDATE reservas_limpia
SET email = NULL
WHERE LOWER(TRIM(email)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET telefono = NULL
WHERE LOWER(TRIM(telefono)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET fecha_cancelacion = NULL
WHERE LOWER(TRIM(fecha_cancelacion)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET impuesto = NULL
WHERE LOWER(TRIM(impuesto)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET motivo_cancelacion = NULL
WHERE LOWER(TRIM(motivo_cancelacion)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET precio_paquete = NULLIF(TRIM(precio_paquete), '');

UPDATE reservas_limpia
SET precio_paquete = NULL
WHERE LOWER(TRIM(precio_paquete)) IN ('n/a','na','s/d');

UPDATE reservas_limpia
SET impuesto = NULLIF(TRIM(impuesto), '');

-- ============================================================
-- 07) NORMALIZACIÓN DE TEXTO: ESPACIOS EXTRA + CASE
--     - Se eliminan dobles espacios y se estandariza a minúsculas
--       donde es útil (email/estado/motivo).
-- ============================================================
UPDATE reservas_limpia
SET
  nombre_cliente     = REGEXP_REPLACE(TRIM(nombre_cliente), '\\s+', ' '),
  destino            = REGEXP_REPLACE(TRIM(destino), '\\s+', ' '),
  pais               = REGEXP_REPLACE(TRIM(pais), '\\s+', ' '),
  canal_venta        = REGEXP_REPLACE(TRIM(canal_venta), '\\s+', ' '),
  tipo_paquete       = REGEXP_REPLACE(TRIM(tipo_paquete), '\\s+', ' '),
  estado_reserva     = LOWER(REGEXP_REPLACE(TRIM(estado_reserva), '\\s+', ' ')),
  email              = LOWER(REGEXP_REPLACE(TRIM(email), '\\s+', ' ')),
  telefono           = LOWER(REGEXP_REPLACE(TRIM(telefono), '\\s+', ' ')),
  motivo_cancelacion = LOWER(REGEXP_REPLACE(TRIM(motivo_cancelacion), '\\s+', ' ')),
  cliente_id         = LOWER(REGEXP_REPLACE(TRIM(cliente_id), '\\s+', ' ')),
  ID_reserva         = LOWER(REGEXP_REPLACE(TRIM(ID_reserva), '\\s+', ' '));

-- ============================================================
-- 08) DUPLICADOS (DETECCIÓN / REPORTE)
--     - En este proyecto se detectan duplicados, pero NO se eliminan
--       automáticamente sin regla de negocio (prioridad/fuente/timestamp).
-- ============================================================

-- Duplicados por clave operativa
SELECT ID_reserva, COUNT(*) AS Duplicados
FROM reservas_limpia
GROUP BY ID_reserva
HAVING COUNT(*) > 1;

-- Duplicados lógicos (ejemplo de agrupación)
SELECT
  cliente_id, ID_reserva, fecha_reserva, destino, tipo_paquete,
  COUNT(*) AS duplicados
FROM reservas_limpia
GROUP BY cliente_id, fecha_reserva, destino, tipo_paquete, ID_reserva
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;

-- ============================================================
-- 09) FECHAS: ESTANDARIZACIÓN (MULTI-FORMATO)
--     - Se crea columna auxiliar DATE para convertir formatos.
--     - Se valida qué registros no pudieron convertirse.
--     - Luego se sobreescribe la columna original con la limpia.
--     Nota: Se desactiva SQL_SAFE_UPDATES para permitir updates masivos.
-- ============================================================
SELECT DISTINCT fecha_viaje, fecha_reserva, fecha_cancelacion
FROM reservas_limpia;

SET SQL_SAFE_UPDATES = 0;

-- 09.1) Prueba de conversión (sin modificar datos)
SELECT
  fecha_viaje,
  CASE
    WHEN fecha_viaje LIKE '____/__/__' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '____-__-__' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%Y-%m-%d'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__/__/____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d/%m/%Y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__/__/__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d/%m/%y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d-%m-%y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%m-%d-%Y'), '%Y-%m-%d')
    ELSE NULL
  END AS nueva_fecha
FROM reservas_limpia;

-- 09.2) Fecha_viaje
ALTER TABLE reservas_limpia ADD fecha_viaje_limpia DATE;

UPDATE reservas_limpia
SET fecha_viaje_limpia =
  CASE
    WHEN fecha_viaje LIKE '____/__/__' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '____-__-__' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%Y-%m-%d'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__/__/____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d/%m/%Y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__/__/__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d/%m/%y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%d-%m-%y'), '%Y-%m-%d')
    WHEN fecha_viaje LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_viaje, '%m-%d-%Y'), '%Y-%m-%d')
    ELSE NULL
  END;

-- Validación: valores que NO se pudieron convertir
SELECT *
FROM reservas_limpia
WHERE fecha_viaje_limpia IS NULL
  AND fecha_viaje IS NOT NULL;

-- Aplicar el resultado
UPDATE reservas_limpia
SET fecha_viaje = fecha_viaje_limpia;

-- 09.3) Fecha_reserva
ALTER TABLE reservas_limpia ADD COLUMN fecha_reserva_limpia DATE;

UPDATE reservas_limpia
SET fecha_reserva_limpia =
  CASE
    WHEN fecha_reserva LIKE '____/__/__' THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '____-__-__' THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%Y-%m-%d'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '__/__/____' THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%d/%m/%Y'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '__/__/__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%d/%m/%y'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '__-__-__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%d-%m-%y'), '%Y-%m-%d')
    WHEN fecha_reserva LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_reserva, '%m-%d-%Y'), '%Y-%m-%d')
    ELSE NULL
  END;

UPDATE reservas_limpia
SET fecha_reserva = fecha_reserva_limpia;

-- 09.4) Fecha_cancelacion
ALTER TABLE reservas_limpia ADD COLUMN cancelacion_limpia DATE;

UPDATE reservas_limpia
SET cancelacion_limpia =
  CASE
    WHEN fecha_cancelacion LIKE '____/__/__' THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '____-__-__' THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%Y-%m-%d'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '__/__/____' THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%d/%m/%Y'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '__/__/__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%d/%m/%y'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '__-__-__'   THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%d-%m-%y'), '%Y-%m-%d')
    WHEN fecha_cancelacion LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(fecha_cancelacion, '%m-%d-%Y'), '%Y-%m-%d')
    ELSE NULL
  END;

UPDATE reservas_limpia
SET fecha_cancelacion = cancelacion_limpia;

-- ============================================================
-- 10) FORMATO DE NÚMEROS: precio_paquete e impuesto -> DECIMAL
--     - Maneja formatos USA y LATAM + símbolos y separadores.
-- ============================================================

-- 10.1) Prueba de conversión precio (sin modificar)
SELECT
  precio_paquete,
  CAST(
    TRIM(
      CASE
        WHEN precio_paquete LIKE '%,%.%' THEN REPLACE(REPLACE(precio_paquete,'$',''),',','')
        WHEN precio_paquete LIKE '%.%,%' THEN REPLACE(REPLACE(REPLACE(precio_paquete,'$',''),'.',''),',','.')
        WHEN precio_paquete LIKE '%,%' AND precio_paquete NOT LIKE '%.%' THEN REPLACE(REPLACE(precio_paquete,'$',''),',','.')
        ELSE REPLACE(REPLACE(precio_paquete,'$',''),' ','')
      END
    ) AS DECIMAL(15,2)
  ) AS precio_limpio
FROM reservas_limpia;

-- 10.2) Aplicación a precio_paquete
ALTER TABLE reservas_limpia ADD COLUMN precio_paq_limp DECIMAL(15,2);

UPDATE reservas_limpia
SET precio_paq_limp =
  CAST(
    TRIM(
      CASE
        WHEN precio_paquete LIKE '%,%.%' THEN REPLACE(REPLACE(precio_paquete,'$',''),',','')
        WHEN precio_paquete LIKE '%.%,%' THEN REPLACE(REPLACE(REPLACE(precio_paquete,'$',''),'.',''),',','.')
        WHEN precio_paquete LIKE '%,%' AND precio_paquete NOT LIKE '%.%' THEN REPLACE(REPLACE(precio_paquete,'$',''),',','.')
        ELSE REPLACE(REPLACE(precio_paquete,'$',''),' ','')
      END
    ) AS DECIMAL(15,2)
  );

SELECT precio_paquete, precio_paq_limp
FROM reservas_limpia;

UPDATE reservas_limpia
SET precio_paquete = precio_paq_limp;

ALTER TABLE reservas_limpia
DROP COLUMN precio_paq_limp;

-- 10.3) Aplicación a impuesto
ALTER TABLE reservas_limpia
ADD COLUMN impuesto_limp DECIMAL(15,2);

UPDATE reservas_limpia
SET impuesto_limp =
  CAST(
    TRIM(
      CASE
        WHEN impuesto LIKE '%,%.%' THEN REPLACE(REPLACE(impuesto,'$',''),',','')
        WHEN impuesto LIKE '%.%,%' THEN REPLACE(REPLACE(REPLACE(impuesto,'$',''),'.',''),',','.')
        WHEN impuesto LIKE '%,%' AND impuesto NOT LIKE '%.%' THEN REPLACE(REPLACE(impuesto,'$',''),',','.')
        ELSE REPLACE(REPLACE(impuesto,'$',''),' ','')
      END
    ) AS DECIMAL(15,2)
  );

UPDATE reservas_limpia
SET impuesto = impuesto_limp;

ALTER TABLE reservas_limpia
DROP COLUMN impuesto_limp;

-- ============================================================
-- 11) REGLAS DE NEGOCIO / CONSISTENCIA (ESTADO vs CANCELACIÓN)
-- ============================================================

-- Evidencia: canceladas con fecha (muestra)
SELECT fecha_cancelacion
FROM reservas_limpia
WHERE estado_reserva = 'cancelada';

-- 11.1) Canceladas sin fecha (se reporta, NO se corrige)
SELECT COUNT(*) AS canceladas_sin_fecha
FROM reservas_limpia
WHERE estado_reserva = 'cancelada'
  AND fecha_cancelacion IS NULL;

-- Nota:
-- Estos registros no fueron eliminados ni modificados para preservar integridad histórica.
-- Se reportan como anomalía de calidad de datos.

-- 11.2) Confirmadas/Pendientes con fecha_cancelacion (contradicción)
SELECT COUNT(*) AS confirmadas_con_cancelacion
FROM reservas_limpia
WHERE estado_reserva IN ('confirmada','pendiente')
  AND fecha_cancelacion IS NOT NULL;

-- Corrección segura: si NO está cancelada/reembolsada, fecha_cancelacion debe ser NULL
UPDATE reservas_limpia
SET fecha_cancelacion = NULL
WHERE estado_reserva IN ('confirmada','pendiente')
  AND fecha_cancelacion IS NOT NULL;

-- 11.3) Canceladas/Reembolsadas sin fecha (se reporta)
SELECT COUNT(*) AS canceladas_reembolsadas_sin_fecha
FROM reservas_limpia
WHERE estado_reserva IN ('cancelada','reembolsada')
  AND fecha_cancelacion IS NULL;

-- ============================================================
-- 12) LIMPIEZA DE COLUMNAS AUXILIARES (TEMPORALES)
--     - Se eliminan columnas utilizadas para parsing/conversión.
-- ============================================================

-- Nota: precio_paq_limp ya fue eliminado arriba.
ALTER TABLE reservas_limpia
DROP COLUMN fecha_viaje_limpia,
DROP COLUMN cancelacion_limpia;

ALTER TABLE reservas_limpia
DROP COLUMN fecha_reserva_limpia;

-- ============================================================
-- 13) QUALITY GATE (VALIDACIONES FINALES PARA REPORTE)
--     - Estas consultas alimentan tu “Reporte de Limpieza”.
-- ============================================================

-- 13.1) Total final
SELECT COUNT(*) AS total_final
FROM reservas_limpia;

-- 13.2) Nulos críticos
SELECT
  SUM(ID_reserva IS NULL) AS id_reserva_null,
  SUM(fecha_reserva IS NULL) AS fecha_reserva_null,
  SUM(fecha_viaje IS NULL) AS fecha_viaje_null,
  SUM(precio_paquete IS NULL) AS precio_null,
  SUM(impuesto IS NULL) AS impuesto_null
FROM reservas_limpia;

-- 13.3) Reglas de negocio: canceladas sin fecha (anomalías)
SELECT
  SUM(estado_reserva IN ('cancelada','reembolsada') AND fecha_cancelacion IS NULL) AS canceladas_sin_fecha_anomalia
FROM reservas_limpia;

-- 13.4) Validación de rangos simples (ejemplo)
SELECT MAX(noches) AS max_noches
FROM reservas_limpia;

-- 13.5) Chequeo de consistencia temporal (solo evidencia, no corrige)
SELECT COUNT(*) AS viaje_antes_reserva
FROM reservas_limpia
WHERE fecha_viaje < fecha_reserva;

