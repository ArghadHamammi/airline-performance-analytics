import pandas as pd
import os
import psycopg2
from io import StringIO

# إعدادات قاعدة البيانات
DB_HOST = "postgres"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

# تحديد السنة والشهر
YEAR = 2024
MONTH = 1

# المسار الكامل لملف البيانات المعالجة
processed_file_path = f"/opt/airflow/data/processed/flights_processed_{YEAR}_{MONTH}.csv"

# قراءة البيانات
print(f"Loading processed data from: {processed_file_path}")
df = pd.read_csv(processed_file_path)

# إعادة تسمية الأعمدة لتتطابق مع أسماء الأعمدة في قاعدة البيانات
df.rename(columns={
    "FlightDate": "flight_date",
    "Reporting_Airline": "reporting_airline",
    "Flight_Number_Reporting_Airline": "flight_number_reporting_airline",
    "Origin": "origin",
    "OriginCityName": "origin_city_name",
    "OriginState": "origin_state",
    "DepDelayMinutes": "dep_delay_minutes",
    "ArrDelayMinutes": "arr_delay_minutes",
    "TotalDelayMinutes": "total_delay_minutes",
    "Cancelled": "cancelled",
    "Diverted": "diverted",
    "AirTime": "air_time",
    "Distance": "distance"
}, inplace=True)

# اختيار الأعمدة المطلوبة فقط
required_columns = [
    "flight_date",
    "reporting_airline",
    "flight_number_reporting_airline",
    "origin",
    "origin_city_name",
    "origin_state",
    "dep_delay_minutes",
    "arr_delay_minutes",
    "total_delay_minutes",
    "cancelled",
    "diverted",
    "air_time",
    "distance"
]

df = df[required_columns]

# ✅ تأكد أن الأعمدة الرقمية المخزنة كـ int
df['cancelled'] = df['cancelled'].fillna(0).astype(int)
df['diverted'] = df['diverted'].fillna(0).astype(int)

# تحميل البيانات إلى قاعدة البيانات
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # تحويل DataFrame إلى ملف مؤقت بصيغة CSV في الذاكرة
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    print("Inserting data into the 'flights' table...")
    cursor.copy_expert(
        sql=f"COPY flights ({', '.join(required_columns)}) FROM STDIN WITH CSV",
        file=buffer
    )

    conn.commit()
    print("✅ Data loaded successfully into 'flights' table.")

except Exception as e:
    print("❌ Error loading data into PostgreSQL:", e)

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
