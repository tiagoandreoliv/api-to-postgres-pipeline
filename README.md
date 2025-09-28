# API to Postgres Pipeline

ETL pipeline that extracts data from a public API, transforms it with **Pandas**, and loads it into **PostgreSQL** using **Docker**.

---

## 🚀 Features
- Extracts data from a free public API (`jsonplaceholder.typicode.com`)
- Transforms raw JSON data into structured tables with Pandas
- Loads data into a PostgreSQL database running in Docker
- Example schema:
  - `raw_posts` → raw extracted data
  - `dim_users` → dimension table with user info
- Easily reproducible with `docker-compose`

---

## 🛠️ Technologies Used
- **Python 3**
- **Pandas**
- **PostgreSQL**
- **Docker & Docker Compose**

---

## ⚡ How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/tiagoandreoliv/api-to-postgres-pipeline.git
   cd api-to-postgres-pipeline
