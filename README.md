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

Najprv boli dáta z pôvodného súboru (formát .csv) načítané do Snowflake cez interné úložisko stage s názvom my_stage. Príkaz na vytvorenie stage:
```sql
CREATE OR REPLACE STAGE imdb_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

Dáta z `.csv` súborov boli nahrané do staging tabuliek v Snowflake pomocou príkazov `COPY INTO`.
```sql
COPY INTO movie_staging
FROM @imdb_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

**2. Transform (Transformácia dát)**

V tomto štádiu boli vytvorené dimenzie.

1. `dim_movie`
Tabuľka obsahuje údaje o filmoch, ktoré boli prevzaté z tabuľky `movie_staging`.

```sql
CREATE OR REPLACE TABLE dim_movie AS 
SELECT DISTINCT
    m.id AS movie_id,
    m.title,
    m.date_published,
    m.duration,
    m.country,
    m.languages,
    m.production_company
FROM movie_staging m;
```

2. `dim_actor`
Tabuľka obsahuje údaje o hercoch, ktoré boli prevzaté z tabuliek `names_staging` a `role_mapping_staging`.

```sql
CREATE OR REPLACE TABLE dim_actor AS
SELECT DISTINCT
    n.id AS actor_id,
    n.name,
    n.height,
    n.date_of_birth,
    n.known_for_movies
FROM names_staging n
JOIN role_mapping_staging dm ON n.id = dm.name_id;
```

3. `dim_director`
Tabuľka obsahuje údaje o režiséroch, ktoré boli prevzaté z tabuliek `names_staging` a `role_mapping_staging`.

```sql
CREATE OR REPLACE TABLE dim_director AS 
SELECT DISTINCT
    d.id AS director_id,
    d.name,
    d.height,
    d.date_of_birth,
    d.known_for_movies
FROM names_staging d
JOIN director_mapping_staging dn ON d.id = dn.name_id;
```

4. `dim_genre`
Tabuľka obsahuje údaje o žánroch, ktoré boli prevzaté z tabuľky `genre_staging`.

```sql
CREATE OR REPLACE TABLE dim_genre AS
SELECT DISTINCT
    g.movie_id AS dim_movie_id,
    g.genre
FROM genre_staging g;
```

5. `dim_date`
Tabuľka obsahuje údaje o dátumoch publikácií filmov, ktoré boli prevzaté z tabuľky movie_staging.

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    id,
    ROW_NUMBER() OVER (ORDER BY CAST(date_published AS DATE)) AS dim_dateID,
    date_published AS full_date,   
    DAY(date_published) AS day,    
    WEEK(date_published) AS week,  
    MONTH(date_published) AS month,
    YEAR(date_published) AS year   
FROM 
    movie_staging;
```

**3. Load (Načítanie dát)**
Transformované dáta boli nahrané do finálnej tabuľky `fact_ratings`, s údajmi o hodnoteniach filmov:

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

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta presunuté do finálnej štruktúry. Na záver boli staging tabuľky vymazané, aby sa optimalizovalo využitie úložiska.

```sql
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS genre_staging;
```

---

## 4. Vizualizacia dat

**1. Filmy s najlepším hodnotením.**

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/DATA_VISUALISATIONS/Top%20rated%20movies.png">
  <br>
  <em>Filmy s najlepším hodnotením</em>
</p>

Táto vizualizácia nám ukazuje 10 najlepších filmov podľa hodnotenia na IMDB. S pomocou toho môžeme zistiť, že napríklad `"Kirket"` a `"Love in Kilnerry"` majú najvyššie hodnotenie, čo naznačuje, že sa pravdepodobne budú páčiť používateľovi.

```sql
CREATE OR REPLACE VIEW top_rated_movies AS
SELECT DISTINCT 
    m.title,
    r.avg_rating
FROM 
    dim_movie m
JOIN 
    fact_ratings r ON m.movie_id = r.fact_movie_id
ORDER BY 
    r.avg_rating DESC
LIMIT 10;
```

---

**2. Najznámejší herci (podľa počtu filmov, v ktorých sa zúčastnili).**

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/DATA_VISUALISATIONS/10%20Most%20popular%20actors.png">
  <br>
  <em>Najznámejší herci (podľa počtu filmov, v ktorých sa zúčastnili)</em>
</p>

Táto vizualizácia nám ukazuje 10 najznámejších hercov podľa počtu filmov, v ktorých sa objavili. S pomocou toho môžeme zhodnotiť profesionalitu a skúsenosti herca alebo pomôcť používateľovi nájsť filmy s konkrétnym hercom. V príklade môžeme zistiť, že najpopulárnejším je `James Franco`.

```sql
CREATE OR REPLACE VIEW popular_actors AS
SELECT 
    a.name AS actor_name,
    COUNT(f.fact_movie_id) AS movie_count
FROM 
    dim_actor a
JOIN 
    fact_ratings f ON a.actor_id = f.dim_actor_id
GROUP BY 
    a.name
ORDER BY 
    movie_count DESC
LIMIT 10;
```

---

**3. Krajiny s najväčším počtom natočených filmov.**

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/DATA_VISUALISATIONS/Countires%20by%20movie%20count.png">
  <br>
  <em>Krajiny s najväčším počtom natočených filmov</em>
</p>

Táto vizualizácia nám ukazuje počet filmov, ktoré natočila každá krajina. S pomocou toho môžeme zistiť, že `Amerika` natočila najviac filmov a má v tom najväčšie skúsenosti. Takisto to môže pomôcť ľuďom z rôznych krajín nájsť informácie o filmoch, ktoré boli natočené v ich rodnej krajine.

```sql
CREATE OR REPLACE VIEW movies_by_country AS
SELECT 
    m.country,
    COUNT(m.movie_id) AS movie_count
FROM 
    dim_movie m
GROUP BY 
    m.country
ORDER BY 
    movie_count DESC
LIMIT 10;
```

---

**4. Priemerná dĺžka filmov.**

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/DATA_VISUALISATIONS/Average%20movie%20duration.png">
  <br>
  <em>Priemerná dĺžka filmov</em>
</p>

Táto vizualizácia nám ukazuje priemernú dĺžku filmov. S pomocou toho môžeme zistiť, že väčšina filmov trvá od `80` do `110` minút, pričom s touto informáciou môže používateľ vypočítať približný čas, ktorý si môže vyhradiť na sledovanie filmu.

```sql
CREATE OR REPLACE VIEW movie_duration_distribution AS
SELECT 
    m.duration AS movie_duration, 
    COUNT(m.movie_id) AS movie_count
FROM 
    dim_movie m
GROUP BY 
    m.duration
ORDER BY 
    movie_duration;
```

---

**5. Najlepší režiséri (podľa priemerného hodnotenia ich filmov).**

<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/DATA_VISUALISATIONS/The%20best%20directors%20by%20rating.png">
  <br>
  <em>Najlepší režiséri (podľa priemerného hodnotenia ich filmov)</em>
</p>

Táto vizualizácia nám ukazuje najlepších režisérov podľa priemerného hodnotenia ich filmov. S pomocou toho môže používateľ vybrať režiséra a pozrieť si jeho film, pričom sa spolieha na jeho profesionalitu.

```sql
CREATE OR REPLACE VIEW top_directors_by_rating AS
SELECT 
    dir.name AS director_name,
    AVG(r.avg_rating) AS avg_rating
FROM 
    dim_director dir
JOIN 
    fact_ratings r ON dir.director_id = r.dim_director_id
GROUP BY 
    dir.name
ORDER BY 
    avg_rating DESC;
```

---

Dashboard zhromažďuje usporiadané údaje a odpovedá na dôležité otázky týkajúce sa výberu filmov, hercov alebo režisérov a hlbšieho analýzy informácií o filmoch. Vizualizácie nám pomáhajú podrobnejšie a jednoduchšie analyzovať údaje a robiť závery pre seba týkajúce sa týchto informácií.

---

**Autor:** Dashchenko Yehor
