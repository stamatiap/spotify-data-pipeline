# 🎧 Spotify Data Pipeline

This project is an end-to-end **Spotify Data Pipeline** that extracts, stores, and transforms Spotify data — including my own playlists, my friends’ playlists, and my listening history — to uncover shared listening patterns and ultimately power a **song recommendation system**.

---

## 🚀 Overview

The pipeline demonstrates a modular and scalable **ELT architecture** using Python, Snowflake, and dbt.  
It automates the process of fetching data from the **Spotify Web API**, loading it into Snowflake, and transforming it into analytics-ready models suitable for exploration, dashboards, and future machine learning applications.

---

## 🧩 Pipeline Flow

1. **Extraction (Python)** – Collect playlists, tracks, artists, and listening history from the Spotify API.  
2. **Loading (Snowflake)** – Load the raw data into Snowflake tables using automated scripts.  
3. **Transformation (dbt)** – Clean and model the data to create marts for insights like track, artist, and playlist popularity.  
4. *(Planned)* **Visualization (Power BI)** – Build interactive dashboards for music trends and user similarities.  
5. *(Planned)* **Orchestration (Airflow)** – Schedule and monitor the pipeline for regular updates.

---

## 🛠️ Tech Stack

- **Python** – for data extraction and ingestion  
- **Snowflake** – data warehouse for scalable storage  
- **SQL** – data querying and exploration  
- **dbt** – data transformations, testing, and modeling  
- *(Future: Power BI, Airflow)*

---

  ## 💡 Future Work

- Develop a **similarity model** to detect shared music tastes between users.  
- Build a **song recommendation engine** using transformed data.  
- Automate and orchestrate the pipeline with **Airflow**.  
- Create a **Power BI dashboard** for playlist and artist analytics.

---
