# Netflix-databrics-project


---

# Netflix Analytics Project (Databricks)

## Overview

This project analyzes Netflix Movies & TV Shows using Databricks.
It includes **data cleaning, transformation, Delta Lake table creation, SQL queries, and dashboards**.

---

## Dataset

* Source: Netflix Titles Dataset
* Columns: show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in (genres), description

---

## Steps

1. **Upload CSV to Databricks**
2. **Clean & Transform Data**

   * Remove duplicates
   * Convert date formats
   * Split genres and country list into arrays
   * Convert release_year to integer
3. **Save Cleaned Data as Delta Table**

   * Table name: `netflix_db.netflix_titles`

---

## SQL Views

**Top 10 Countries by Shows**

```sql
SELECT country_name, COUNT(*) AS total_shows
FROM netflix_db.netflix_titles
LATERAL VIEW explode(country_list) AS country_name
GROUP BY country_name
ORDER BY total_shows DESC
LIMIT 10;
```

**Count by Type**

```sql
SELECT type, COUNT(*) AS total_shows
FROM netflix_db.netflix_titles
GROUP BY type;
```

**Top 10 Genres**

```sql
SELECT genre_name, COUNT(*) AS total_shows
FROM netflix_db.netflix_titles
LATERAL VIEW explode(genres) AS genre_name
GROUP BY genre_name
ORDER BY total_shows DESC
LIMIT 10;
```

**Top 10 Directors**

```sql
SELECT director, COUNT(*) AS total_shows
FROM netflix_db.netflix_titles
WHERE director IS NOT NULL
GROUP BY director
ORDER BY total_shows DESC
LIMIT 10;
```

**Count by Rating**

```sql
SELECT rating, COUNT(*) AS total_shows
FROM netflix_db.netflix_titles
GROUP BY rating
ORDER BY total_shows DESC;
```

---

## Dashboard

* Top countries (bar chart)
* Movie vs TV Shows (pie/bar chart)
* Top genres (bar chart)
* Ratings distribution (bar chart)
* Top 10 Directors by Number of Shows

---

## How to Run

1. Upload `netflix_titles.csv` to Databricks
2. Run the notebook to clean and create Delta table
3. Execute SQL queries
4. Build dashboards

---

## Author

Venu – EU Business School Munich

