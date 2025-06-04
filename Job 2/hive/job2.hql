DROP TABLE IF EXISTS used_cars;

CREATE TABLE used_cars (
    city STRING,
    daysonmarket INT,
    description STRING,
    price FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '${hivevar:input_path}' OVERWRITE INTO TABLE used_cars;
ADD FILE words_count.py;
ADD FILE top3.py;


CREATE TABLE IF NOT EXISTS transform_1 AS
    SELECT 
        city, 
        year,
        CASE 
            WHEN price < 20000 THEN 'basso'
            WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
            ELSE 'alto'
        END AS categoria,
        daysonmarket,
        description
    FROM used_cars;


CREATE TABLE IF NOT EXISTS transform_2 AS
    SELECT
        TRANSFORM(
            city, year, categoria, daysonmarket, description
        ) USING 'python3 words_count.py'
        AS city, year, categoria, daysonmarket, word_count
    FROM transform_1;


CREATE TABLE IF NOT EXISTS transform_3 AS
    SELECT
        city,
        year,
        categoria,
        COUNT(*) AS num_cars,
        AVG(daysonmarket) AS avg_giorni,
        COLLECT_SET(word_count) AS lista_parole
    FROM transform_2
    GROUP BY city, year, categoria;


CREATE TABLE IF NOT EXISTS transform_4 AS
SELECT
    TRANSFORM(
        city, year, tier, num_cars, avg_daysonmarket, word_count_list
    ) USING 'python3 top_3_words.py'
    AS city, year, tier, num_cars, avg_daysonmarket, top_words
FROM transform_3;


SELECT *
FROM transform_4
LIMIT 10;

DROP TABLE used_cars;
DROP TABLE transform_1;
DROP TABLE transform_2;
DROP TABLE transform_3;