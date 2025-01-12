# Dokumentácia k implementácii ETL procesu v Snowflake

Téma projektu sa zameriava na analýzu filmových dát z databázy podobnej IMDB. Hlavným cieľom je umožniť analýzu kľúčových metrík, ako sú hodnotenia filmov, obľúbenosť hercov a režisérov, trendy v žánroch a regionálna distribúcia produkcie filmov.

---

## 1. Uvod a popis zdrojovych dat
Téma projektu sa zameriava na analýzu filmových dát z verejného datasetu, ktorý obsahuje informácie o filmoch, ich hodnoteniach, hercoch, režiséroch a žánroch. Cieľom projektu je identifikovať najlepšie hodnotené filmy, obľúbených hercov a režisérov a analyzovať preferencie divákov na základe žánrov a krajín produkcie.

### Zdrojové dáta

Dataset obsahuje nasledujúce tabuľky:
- `movie.csv`: Informácie o filmoch (ID, názov, trvanie, rok vydania, krajina produkcie, ID žánru).
- `ratings.csv`: Hodnotenia filmov (ID filmu, priemerné hodnotenie, počet hodnotení).
- `names.csv`: Zoznam osôb (ID osoby, meno, dátum narodenia, profesia).
- `role_mapping.csv`: Mapovanie rolí osôb vo filmoch (ID osoby, ID filmu, rola).
- `director_mapping.csv`: Mapovanie režisérov k filmom (ID režiséra, ID filmu).
- `genre.csv`: Informácie o žánroch (ID žánru, názov žánru).

### ERD diagram

Nižšie je znázornený ERD diagram pôvodnej štruktúry zdrojových dát:
<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/IMDB_ERD.png">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDB</em>
</p>

---
## Dimenzionalny model

Pre projekt bol navrhnutý hviezdicový model (star schema), ktorý zahŕňa faktovú tabuľku **`fact_ratings`** a nasledujúce dimenzie:
- **`dim_movie`**: Informácie o filmoch (ID, názov, dátum publikovania, dĺžka, krajina, jazyk, produkčná spoločnosť).
- **`dim_actor`**: Informácie o hercoch (ID, meno, výška, dátum narodenia, známe filmy).
- **`dim_director`**: Informácie o režiséroch (ID, meno, výška, dátum narodenia, známe filmy).
- **`dim_genre`**: Informácie o žánroch (ID filmu, žáner).
- **`dim_date`**: Informácie o dátumoch (deň, tyždeň, mesiac, rok).

Štruktúra hviezdicového modelu je zobrazená na diagrame nižšie. Tento diagram ukazuje vzťahy medzi faktovou tabuľkou a dimenziami, čo uľahčuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/STAR_SCHEME.png">
  <br>
  <em>Obrázok 2 Star Schema pre IMDB</em>
</p>

---
## 3. ETL proces v Snowflake

Kroky ETL procesu

**1. Extract (Extrahovanie dát)**
Dáta z `.csv` súborov boli nahrané do staging tabuliek v Snowflake pomocou príkazov `COPY INTO`.
```sql
COPY INTO movie_staging
FROM @imdb_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

**2. Transform (Transformácia dát)**
Dáta boli transformované do dimenzií a faktovej tabuľky:

```sql
CREATE OR REPLACE TABLE dim_movie AS 
SELECT DISTINCT
    id AS movie_id,
    title,
    date_published,
    duration,
    country,
    languages,
    production_company
FROM movie_staging;
```

**3. Load (Načítanie dát)**
Transformované dáta boli nahrané do finálnych tabuliek:

```sql
CREATE OR REPLACE TABLE fact_ratings AS
SELECT DISTINCT
    r.movie_id AS fact_movie_id,
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    MAX(r.avg_rating) OVER () AS max_rating,
    MIN(r.avg_rating) OVER () AS min_rating,
    d.movie_id AS dim_movie_id,
    a.actor_id AS dim_actor_id,
    dir.director_id AS dim_director_id,
    g.genre AS dim_genre_id,
    dat.dim_dateID AS dim_date_id
FROM ratings_staging r
LEFT JOIN dim_movie d ON r.movie_id = d.movie_id
LEFT JOIN role_mapping_staging rm ON r.movie_id = rm.movie_id
LEFT JOIN dim_actor a ON rm.name_id = a.actor_id
LEFT JOIN director_mapping_staging dm ON r.movie_id = dm.movie_id
LEFT JOIN dim_director dir ON dm.name_id = dir.director_id
LEFT JOIN genre_staging gs ON r.movie_id = gs.movie_id
LEFT JOIN dim_genre g ON gs.genre = g.genre
LEFT JOIN dim_date dat ON d.date_published = dat.full_date;
```
