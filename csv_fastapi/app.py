from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session  
import pandas as pd
from sqlalchemy import text
from models import Student
import models
from database import engine, Base, SessionLocal

# -----------------------------
# DB Setup
# -----------------------------
Base.metadata.create_all(bind=engine)

app = FastAPI()

# -----------------------------
# Load CSV Data
# -----------------------------
try:
    df = pd.read_csv(r"C:\Users\Shaur\OneDrive\Desktop\ANKITA_CSV_TO_FASTAPI\csv_fastapi\students_complete.csv")

    if 'gpa' in df.columns:
        df['gpa'] = df['gpa'].fillna(0)

    print("✅ Data Loaded Successfully")

except Exception as e:
    print("Error:", e)
    df = pd.DataFrame()

# -----------------------------
# Dependency for DB Session
# -----------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------------
# Home API
# -----------------------------
@app.get("/")
def home():
    return {"message": "FastAPI is running with MySQL 🚀"}

# -----------------------------
# ✅ HEALTH CHECK ENDPOINT
# -----------------------------
@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        # ✅ FIXED LINE
        db.execute(text("SELECT 1"))

        data_status = "loaded" if not df.empty else "not loaded"

        return {
            "status": "healthy",
            "database": "connected",
            "dataframe": data_status
        }

    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

# -----------------------------
# Get All Students (CSV)
# -----------------------------
@app.get("/data")
def get_data():
    return df.to_dict(orient="records")

# -----------------------------
# Get Specific Student by ID
# -----------------------------
@app.get("/student/{student_id}")
def get_student(student_id: str):

    print("Received:", student_id)

    result = df[df["student_id"] == student_id]

    if len(result) > 0:
        return result.to_dict(orient="records")
    else:
        return {"message": "Student not found"}

# -----------------------------
# Get All Students (DB)
# -----------------------------
@app.get("/students")
def get_students(db: Session = Depends(get_db)):
    students = db.query(models.Student).all()
    return students
# -----------------------------
# Get Students by Age Greater Than (CSV)
# -----------------------------

@app.get("/students/age-greater-than/{age}")
def get_students_age_greater(age: int):
    result = df[df["age"] > age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}
# -----------------------------
# Get Students by Age Lesser Than (CSV)
# -----------------------------
    
@app.get("/students/age-lesser-than/{age}")
def get_students_age_lesser(age: int):
    result = df[df["age"] < age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}
# -----------------------------
#✅ Filter by City(CSV)
# -----------------------------    

@app.get("/students/city/{city}")
def get_students_by_city(city: str):
    result = df[df["city"].str.lower() == city.lower()]
    return result.to_dict(orient="records")
# -----------------------------
#👉 Age > X AND GPA > Y
# ----------------------------- 
@app.get("/students/filter_greater_both_age_gpa")
def filter_students(age: int, gpa: float):
    result = df[(df["age"] > age) & (df["gpa"] > gpa)]
    return result.to_dict(orient="records")

# -----------------------------
#👉 Age < X AND GPA < Y
# ----------------------------- 
@app.get("/students/filter_lesser_both_age_gpa")
def filter_students(age: int, gpa: float):
    result = df[(df["age"] < age) & (df["gpa"] < gpa)]
    return result.to_dict(orient="records")
