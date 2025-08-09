# Airline Performance Analytics Project

---

### **Project Description**

This project is a complete data engineering pipeline designed to collect, process, and analyze airline performance data in the United States. The goal of this project is to build a robust and automated infrastructure capable of pulling data from multiple sources, storing it, transforming it into analyzable information, and loading it into a data warehouse.

The project is built on the principles of **ETL (Extract, Transform, Load)** and integrates advanced tools like Docker and Apache Airflow to automate the entire workflow.

### **Key Features**

* **Full Automation:** Using Apache Airflow to schedule and run the data pipeline daily.
* **Multiple Data Sources:** Ingesting data from CSV files (flight data) and APIs (weather data).
* **Containerized Environment:** Utilizing Docker Compose to run all services (Airflow, PostgreSQL, Redis) in an isolated and portable environment.
* **Centralized Storage:** Storing processed data in a PostgreSQL database optimized for analytics.
* **Organized Structure:** A clear folder structure to separate raw data, scripts, and logs.

### **Technologies and Tools Used**

| Category                  | Tool/Technology               | Description                                                 |
| ------------------------- | ----------------------------- | ----------------------------------------------------------- |
| **Orchestration** | Apache Airflow                | For managing and scheduling data pipeline tasks.            |
| **Containerization** | Docker & Docker Compose       | For creating a consistent and portable working environment. |
| **Database** | PostgreSQL                    | For storing clean and structured data.                      |
| **Message Broker** | Redis                         | As a message broker for the Airflow Celery Executor.        |
| **Programming Language** | Python                        | For all data ingestion, processing, and transformation tasks. |
| **Libraries** | Pandas, Requests, psycopg2-binary | For data manipulation, API calls, and database connectivity. |

### **Project Structure**

data_pipeline_project/
├── config/                  # Apache Airflow configuration files
├── dags/                    # DAG files for the Airflow pipeline
├── data/                    # Folder for storing raw and processed data
│   ├── raw/
│   └── processed/
├── logs/                    # Airflow task logs
├── plugins/                 # Custom Airflow plugins (if any)
├── scripts/                 # Python scripts for data ingestion, processing, and loading
├── .env                     # Environment variables (not uploaded to GitHub)
├── .gitignore               # Files and folders to be ignored by Git
├── docker-compose.yaml      # Docker service definitions for Airflow and PostgreSQL
├── Dockerfile               # Custom Docker image for Airflow with required libraries
└── README.md                # This file


### **How to Run the Project**

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/YourUsername/your-repository-name.git](https://github.com/YourUsername/your-repository-name.git)
    cd your-repository-name
    ```

2.  **Configure Environment Variables:**
    Ensure you have a `.env` file in the root directory.

3.  **Build and Run Docker Services:**
    ```bash
    docker-compose up -d
    ```

4.  **Access the Airflow UI:**
    Open your web browser and go to `http://localhost:8080`.
    * **Username:** `airflow`
    * **Password:** `airflow`

---

### **Author**
* **Name:** Arghad Affan Hamammi
* **LinkedIn:** [www.linkedin.com/in/arghad-hamammi-111562314](http://www.linkedin.c
