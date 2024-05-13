USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE demosdb;
USE SCHEMA raw_sch;

select * from music;

SELECT 
    trim(regexp_replace(artist,'[^a-zA-Z0-9? ]+')) AS cantante,
    trim(regexp_replace(song, '[^a-zA-Z0-9?,. ]+')) AS canciones,
    trim(regexp_replace(text,'[^a-zA-Z0-9?,.: ]+')) AS letra,
FROM music
ORDER BY cantante asc;

DROP TABLE spotify_clean;

CREATE OR REPLACE TABLE demosdb.refined_sch.music_clean AS
    SELECT 
        trim(regexp_replace(artist,'[^a-zA-Z0-9? ]+')) AS cantante,
        trim(regexp_replace(song, '[^a-zA-Z0-9?,. ]+')) AS canciones,
        trim(regexp_replace(text,'[^a-zA-Z0-9?,.: ]+')) AS letra,
    FROM music
    ORDER BY cantante asc;

alter table demosdb.refined_sch.music_clean add search optimization;

SELECT 
    genero,
    count(*)
FROM demosdb.refined_sch.music_clean
GROUP BY genero;

SELECT
    REPLACE(SNOWFLAKE.CORTEX.TRANSLATE(SNOWFLAKE.CORTEX.SUMMARIZE(LETRA),'en','es'), '&quot;', '"') AS RESUMEN,
FROM
    demosdb.refined_sch.music_clean
WHERE
    CANTANTE = 'ABBA'
    AND CANCIONES = 'As Good As New';


SELECT
    SNOWFLAKE.CORTEX.TRANSLATE(SUBSTRING(LETRA,1000),IDIOMA,'es') AS traduccion
FROM
    DEMOSDB.REFINED_SCH.MUSIC_CLEAN
WHERE
    CANTANTE = 'ABBA'
    AND CANCIONES = 'As Good As New';