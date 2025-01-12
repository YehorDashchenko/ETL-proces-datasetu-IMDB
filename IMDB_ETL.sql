CREATE DATABASE imdb_db_bullfrog;
CREATE SCHEMA imdb_db_bullfrog.staging;
USE SCHEMA imdb_db_bullfrog.staging;

CREATE OR REPLACE TABLE movie_staging (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(250),
    worlwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);

CREATE OR REPLACE TABLE names_staging (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);

CREATE OR REPLACE TABLE role_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    category VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (name_id) REFERENCES names_staging(id)
);

CREATE OR REPLACE TABLE ratings_staging (
    movie_id VARCHAR(10),
    avg_rating DECIMAL(3,1),
    total_votes INT,
    median_rating INT,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id)
);

CREATE OR REPLACE TABLE director_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (name_id) REFERENCES names_staging(id)
);

CREATE OR REPLACE TABLE genre_staging (
    movie_id VARCHAR(10),
    genre VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id)
);

CREATE OR REPLACE STAGE imdb_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO movie_staging
FROM @imdb_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO names_staging
FROM @imdb_stage/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO role_mapping_staging
FROM @imdb_stage/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @imdb_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO director_mapping_staging
FROM @imdb_stage/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genre_staging
FROM @imdb_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT * FROM movie_staging;
SELECT * FROM names_staging;
SELECT * FROM role_mapping_staging;
SELECT * FROM ratings_staging;
SELECT * FROM director_mapping_staging;
SELECT * FROM genre_staging;

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

CREATE OR REPLACE TABLE dim_actor AS
SELECT DISTINCT
    n.id AS actor_id,
    n.name,
    n.height,
    n.date_of_birth,
    n.known_for_movies
FROM names_staging n
JOIN role_mapping_staging dm ON n.id = dm.name_id;

CREATE OR REPLACE TABLE dim_director AS 
SELECT DISTINCT
    d.id AS director_id,
    d.name,
    d.height,
    d.date_of_birth,
    d.known_for_movies
FROM names_staging d
JOIN director_mapping_staging dn ON d.id = dn.name_id;

CREATE OR REPLACE TABLE dim_genre AS
SELECT DISTINCT
    g.movie_id AS dim_movie_id,
    g.genre
FROM genre_staging g;

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

CREATE OR REPLACE TABLE fact_ratings AS
SELECT DISTINCT
    r.movie_id AS fact_movie_id,
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    MAX(r.avg_rating) OVER () AS max_rating,
    MIN(r.avg_rating) OVER () AS min_rating,
    d.movie_id AS dim_movie_id,
    d.duration AS movie_duration,
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
LEFT JOIN dim_genre g ON gs.genre = g.genre;
LEFT JOIN dim_date dat ON d.date_published = dat.full_date;

SELECT * FROM dim_movie;
SELECT * FROM dim_actor;
SELECT * FROM dim_director;
SELECT * FROM dim_genre;
SELECT * FROM dim_date;
SELECT * FROM fact_ratings;

DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS genre_staging;

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


SELECT * FROM top_rated_movies;
SELECT * FROM popular_actors;
SELECT * FROM movies_by_country;
SELECT * FROM movie_duration_distribution;
SELECT * FROM top_directors_by_rating;