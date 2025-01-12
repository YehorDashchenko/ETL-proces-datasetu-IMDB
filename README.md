# Dokumentácia k implementácii ETL procesu v Snowflake

Téma projektu sa zameriava na analýzu filmových dát z databázy podobnej IMDB. Hlavným cieľom je umožniť analýzu kľúčových metrík, ako sú hodnotenia filmov, obľúbenosť hercov a režisérov, trendy v žánroch a regionálna distribúcia produkcie filmov.

---

## 1. Uvod a popis zdrojovych dat
Téma projektu sa zameriava na analýzu filmových dát z verejného datasetu, ktorý obsahuje informácie o filmoch, ich hodnoteniach, hercoch, režiséroch a žánroch. Cieľom projektu je identifikovať najlepšie hodnotené filmy, obľúbených hercov a režisérov a analyzovať preferencie divákov na základe žánrov a krajín produkcie.

### Zdrojové dáta

Dataset obsahuje nasledujúce tabuľky:
- 'movie.csv': Informácie o filmoch (ID, názov, trvanie, rok vydania, krajina produkcie, ID žánru).
- 'ratings.csv': Hodnotenia filmov (ID filmu, priemerné hodnotenie, počet hodnotení).
- 'names.csv': Zoznam osôb (ID osoby, meno, dátum narodenia, profesia).
- 'role_mapping.csv': Mapovanie rolí osôb vo filmoch (ID osoby, ID filmu, rola).
- 'director_mapping.csv': Mapovanie režisérov k filmom (ID režiséra, ID filmu).
- 'genre.csv': Informácie o žánroch (ID žánru, názov žánru).

### ERD diagram

Nižšie je znázornený ERD diagram pôvodnej štruktúry zdrojových dát:
<p align="center">
  <img src="https://github.com/YehorDashchenko/ETL-proces-datasetu-IMDB/blob/main/IMDB_ERD.png">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDB</em>
</p>
