import pandas as pd
import os

# مسارات مطلقة للبيانات داخل بيئة Airflow
raw_data_dir = "/opt/airflow/data/raw"
processed_data_dir = "/opt/airflow/data/processed"

# التأكد من وجود مجلد البيانات المعالجة، وإن لم يوجد، يتم إنشاؤه
os.makedirs(processed_data_dir, exist_ok=True)

# تحديد السنة والشهر لملف البيانات
YEAR = 2024
MONTH = 1

# بناء مسار الملف الخام
raw_file_path = os.path.join(raw_data_dir, f"flights_{YEAR}_{MONTH}.csv")

# 1. قراءة البيانات الخام
try:
    print(f"Reading raw data from {raw_file_path}...")
    df_raw = pd.read_csv(raw_file_path, low_memory=False)
    print("Raw data loaded successfully.")
except FileNotFoundError:
    error_message = f"Error: Raw data file not found at {raw_file_path}. Please run get_flight_data.py first."
    print(error_message)
    raise FileNotFoundError(error_message)

# 2. اختيار الأعمدة المهمة فقط
important_columns = [
    'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek',
    'FlightDate', 'Reporting_Airline', 'Tail_Number', 'Flight_Number_Reporting_Airline',
    'Origin', 'OriginCityName', 'OriginState', 'Dest', 'DestCityName', 'DestState',
    'DepTime', 'DepDelayMinutes', 'DepDelay',
    'ArrTime', 'ArrDelayMinutes', 'ArrDelay',
    'Cancelled', 'CancellationCode', 'Diverted',
    'AirTime', 'Distance'
]

df_transformed = df_raw[important_columns].copy()

# 3. تنظيف البيانات
df_transformed.dropna(subset=['DepTime', 'ArrTime', 'DepDelayMinutes', 'ArrDelayMinutes'], inplace=True)

df_transformed['DepDelayMinutes'] = df_transformed['DepDelayMinutes'].astype(int)
df_transformed['ArrDelayMinutes'] = df_transformed['ArrDelayMinutes'].astype(int)

# ✅ تحويل الأعمدة Cancelled و Diverted إلى int
if 'Cancelled' in df_transformed.columns:
    df_transformed['Cancelled'] = df_transformed['Cancelled'].fillna(0).astype(int)
if 'Diverted' in df_transformed.columns:
    df_transformed['Diverted'] = df_transformed['Diverted'].fillna(0).astype(int)

# 4. إنشاء أعمدة جديدة (Feature Engineering)
df_transformed['IsDelayed'] = df_transformed['ArrDelayMinutes'] > 0
df_transformed['TotalDelayMinutes'] = df_transformed['ArrDelayMinutes'] + df_transformed['DepDelayMinutes']
df_transformed['TotalDelayMinutes'] = df_transformed['TotalDelayMinutes'].apply(lambda x: x if x > 0 else 0)

# 5. حفظ البيانات النظيفة
processed_file_path = os.path.join(processed_data_dir, f"flights_processed_{YEAR}_{MONTH}.csv")
df_transformed.to_csv(processed_file_path, index=False)

print("\nData transformation completed successfully!")
print(f"Cleaned data saved to: {processed_file_path}")
print("First 5 rows of the processed data:")
print(df_transformed.head())
