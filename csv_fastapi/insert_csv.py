import pandas as pd
from sqlalchemy import create_engine

# Load CSV
df = pd.read_csv("students_complete.csv")

# Clean data (important)
df['student_id'] = df['student_id'].astype(str).str.strip()
df['gpa'] = df['gpa'].fillna(0)

# Connect MySQL
engine = create_engine("mysql+pymysql://root:root@localhost:3306/student_db")

# Insert into MySQL
df.to_sql("students", con=engine, if_exists="append", index=False)

print("✅ 110 Records Inserted Successfully")