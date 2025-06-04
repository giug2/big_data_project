DROP TABLE IF EXISTS used_cars;

CREATE EXTERNAL TABLE used_cars (
    make_name STRING,
    model_name STRING,
    price FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '${hivevar:input_path}' OVERWRITE INTO TABLE used_cars;

SELECT
    make_name,
    model_name,
    COUNT(*) AS numero_auto,
    MIN(price) AS prezzo_minimo,
    MAX(price) AS prezzo_massimo,
    ROUND(AVG(price), 2) AS prezzo_medio,
    COLLECT_SET(year) AS anni_presenti
FROM used_cars
GROUP BY make_name, model_name
ORDER BY make_name, model_name
LIMIT 10;

DROP TABLE used_cars;