import os
import requests
import zipfile
import pandas as pd
from io import BytesIO

# تحديد السنة والشهر
YEAR = 2024
MONTH = 1

# استخدام مسار مطلق داخل بيئة Airflow
data_dir = "/opt/airflow/data/raw"
zip_file_path = os.path.join(data_dir, f"flights_{YEAR}_{MONTH}.zip")
csv_file_path = os.path.join(data_dir, f"flights_{YEAR}_{MONTH}.csv")

# التأكد من وجود مجلد حفظ البيانات
os.makedirs(data_dir, exist_ok=True)

# 1. تنزيل الملف المضغوط
print(f"Downloading data from https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YEAR}_{MONTH}.zip...")
try:
    url = f"https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YEAR}_{MONTH}.zip"
    response = requests.get(url, stream=True)
    response.raise_for_status()  # التأكد من نجاح الطلب

    with BytesIO(response.content) as zip_file:
        print("Download completed successfully.")
        
        # 2. فك ضغط الملف مباشرة من الذاكرة
        print("Unzipping the file...")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            # البحث عن ملف CSV داخل الملف المضغوط
            csv_filename = [name for name in zip_ref.namelist() if name.endswith('.csv')][0]
            
            # استخراج الملف وتخزينه في المسار المطلوب
            with zip_ref.open(csv_filename) as source_file:
                with open(csv_file_path, "wb") as destination_file:
                    destination_file.write(source_file.read())
            
            print(f"File extracted to: {csv_file_path}")

except requests.exceptions.RequestException as e:
    print(f"Error downloading the file: {e}")
    exit(1)
except (zipfile.BadZipFile, IndexError) as e:
    print(f"Error unzipping the file: {e}")
    exit(1)

# 3. التأكد من نجاح العملية
if os.path.exists(csv_file_path):
    print("\nData collection successful!")
    print(f"Data for {YEAR}-{MONTH} is ready.")
    
    # قراءة أول 5 صفوف للتأكد من المحتوى
    try:
        df = pd.read_csv(csv_file_path, nrows=5)
        print("First 5 rows of the dataset:")
        print(df.head())
    except pd.errors.ParserError as e:
        print(f"Error reading the CSV file: {e}")
else:
    print("Failed to find the extracted CSV file.")
