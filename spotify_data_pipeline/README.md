# 🧠 dbt Project – Spotify Data Pipeline

This dbt project contains the **transformation layer** of the Spotify Data Pipeline.  
It models and organizes data loaded into Snowflake from the Spotify API, transforming it into analytics-ready tables for analysis, dashboards, and recommendation insights.

---

## 🎯 Purpose

- **Staging models**: clean and map raw Spotify data into well-structured intermediate tables.  
- **Marts**: aggregate and analyze metrics such as track, playlist, and artist popularity to support insights and dashboards.

---

## 🛠️ Usage

Inside the `spotify_data_pipeline/` folder:

- Run transformations: ``` dbt run```.
- Test and validate models: ```dbt test```.
