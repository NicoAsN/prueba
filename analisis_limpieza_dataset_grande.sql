USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE demosdb;
USE SCHEMA raw_sch;

select * from music;

SELECT 
    trim(regexp_replace(artist,'[^a-zA-Z0-9? ]+')) AS cantante,
    trim(regexp_replace(title, '[^a-zA-Z0-9?,. ]+')) AS canciones,
    trim(regexp_replace(lyrics,'[^a-zA-Z0-9?,.: ]+')) AS letra,
    year AS anio,
    case
        when language is not null then language
        when language_ft is not null then language_ft
        when language_cld3 is not null then language_cld3
        else 'en'
    end AS idioma,
    views AS reproducciones
FROM music
ORDER BY cantante asc;

DROP TABLE spotify_clean;

CREATE TABLE demosdb.refined_sch.music_clean AS
    SELECT 
        trim(regexp_replace(artist,'[^a-zA-Z0-9? ]+')) AS cantante,
        trim(regexp_replace(title, '[^a-zA-Z0-9?,. ]+')) AS canciones,
        trim(regexp_replace(lyrics,'[^a-zA-Z0-9?,.: ]+')) AS letra,
        year AS anio,
        case
            when language is not null then language
            when language_ft is not null then language_ft
            when language_cld3 is not null then language_cld3
            else 'en'
        end AS idioma,
        views AS reproducciones
    FROM music
    ORDER BY cantante asc;

SELECT 
    *
FROM demosdb.refined_sch.music_clean;

SELECT
    REPLACE(SNOWFLAKE.CORTEX.TRANSLATE(SNOWFLAKE.CORTEX.SUMMARIZE(LETRA),'en','es'), '&quot;', '"') AS RESUMEN,
FROM
    demosdb.refined_sch.music_clean
WHERE
    CANTANTE = 'ABBA'
    AND CANCIONES = 'As Good As New';