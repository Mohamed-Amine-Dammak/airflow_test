# Apache Airflow – Docker Setup

This project contains a **Dockerized Apache Airflow environment** for orchestrating workflows and ETL pipelines.

It includes:

* `dags/` → Airflow DAGs
* `plugins/` → Custom plugins and operators
* `config/` → Configuration files
* `logs/` → Airflow logs (not committed to Git)
* `docker-compose.yaml` → Airflow services definition

---

## 📌 Project Structure

```
.
├── config/
├── dags/
├── logs/
├── plugins/
├── docker-compose.yaml
└── README.md
```

---

## 🚀 Getting Started

### 1️⃣ Prerequisites

* Docker
* Docker Compose
* Git

---

### 2️⃣ Clone the repository

```bash
git clone https://github.com/Mohamed-Amine-Dammak/airflow.git
cd airflow
```

---

### 3️⃣ Start Airflow

```bash
docker compose up -d
```

First time only (if needed):

```bash
docker compose up airflow-init
```

---

### 4️⃣ Access Airflow UI

Open your browser:

```
http://localhost:8080
```

Default credentials (if not changed):

```
Username: airflow
Password: airflow
```

---

## 📂 Adding a DAG

1. Place your DAG file inside:

```
dags/
```

2. Airflow will automatically detect it.
3. Refresh the UI and enable the DAG.

---

## 🔐 Environment Variables

If needed, create a `.env` file:

```
AIRFLOW_UID=50000
```

(Do not commit `.env` to GitHub)

---

## 🧪 Testing

To check logs:

```bash
docker compose logs -f
```

To stop services:

```bash
docker compose down
```

---

## 📊 Use Cases

This Airflow setup can be used for:

* ETL orchestration
* Triggering n8n workflows
* Triggering Talend Cloud jobs
* Monitoring external APIs
* Cloud Run integrations
* BigQuery / GCS automation
* DevOps scheduling tasks

---

## 🛠 Tech Stack

* Apache Airflow
* Docker
* Docker Compose
* Python
* REST APIs

---

## 📌 Best Practices

* Avoid heavy code at DAG top-level
* Use `@dag` decorator (Airflow 2+ / 3 style)
* Store credentials in Airflow Connections
* Use retries and email alerts
* Keep logs and secrets out of Git

---

# Airflow Connections Configuration

Airflow connections can be defined in a `connections.json` file and automatically synced to your Airflow instance. Each connection has a unique `conn_id` and required fields.

## Example `connections.json`

```jsonc
{
    "_comment": "This file defines Airflow connections. Each key is a 'conn_id' used in Airflow.",
    
    "my_postgres": {
        "_comment": "Postgres database connection example",
        "conn_type": "postgres",          // Type of connection: postgres, mysql, etc.
        "host": "localhost",              // Database host
        "login": "username",              // Database username
        "password": "password",           // Database password
        "schema": "mydb",                 // Database schema or name
        "extra": "{\"port\": 5432}"       // Optional extra JSON parameters (e.g., port, sslmode)
    },

    "my_api": {
        "_comment": "HTTP API connection example",
        "conn_type": "http",              // Type of connection: http, https, etc.
        "login": "user",                  // API username (if required)
        "password": "pass",               // API password or token (if required)
        "extra": "{\"headers\":{\"Authorization\":\"Bearer TOKEN\"}}"  
        // Optional extra JSON, e.g., HTTP headers for Authorization
    }
}
````

## Field Descriptions

| Field       | Description                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------ |
| `conn_id`   | Unique identifier for this connection, referenced in Airflow DAGs.                               |
| `conn_type` | Type of connection. Must match the Airflow provider installed (e.g., postgres, mysql, http, s3). |
| `host`      | Hostname or IP address of the service (optional for some types like HTTP).                       |
| `login`     | Username or API user.                                                                            |
| `password`  | Password, token, or secret. Can be replaced with an environment variable for security.           |
| `schema`    | Database name or schema (optional).                                                              |
| `extra`     | Additional configuration in JSON format. Examples: port, SSL mode, HTTP headers.                 |
| `_comment`  | Optional field to document the purpose of each connection. Ignored by the sync script.           |

## Usage Notes

* **Sync to Airflow**: Use the `sync_connections.py` script to automatically create or update these connections inside your Airflow instance.
* **Secrets Management**: Do **not** store sensitive passwords in GitHub plaintext. Use environment variables or an encrypted file.
* **Extensible**: You can add more connections by adding new keys to the JSON file following the same format.

💡 **Tip**: For passwords, you can use placeholders like:

```jsonc
"password": "${MY_POSTGRES_PASSWORD}"
```

Then your Python sync script can read the value from environment variables instead of storing plaintext credentials.

#### 👤 Mohamed Amine Dammak, Engineering Student – Data Engineering & AI

