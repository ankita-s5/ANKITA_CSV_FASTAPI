from fastapi import FastAPI, Depends
import crud
from typing import List
from fastapi import HTTPException
from schema import StudentCreate, StudentUpdate
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
    df = pd.read_csv(r"C:\Users\LENOVO\Downloads\students_complete.csv")

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
# ✅ CREATE API
# -----------------------------
@app.post("/students")
def create_student(student: StudentCreate, db: Session = Depends(get_db)):
    return crud.create_student(db, student.dict())


# -----------------------------
# ✅ Get All Students (CSV)
# -----------------------------
@app.get("/students-db")
def get_students(db: Session = Depends(get_db)):
    return crud.get_all_students(db)

# -----------------------------
# Get Specific Student by ID
# -----------------------------
@app.get("/students-db/{student_id}")
def get_student(student_id: str, db: Session = Depends(get_db)):
    student = crud.get_student_by_id(db, student_id)

    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    return student

# -----------------------------
# Update Student by ID  
# -----------------------------
@app.put("/students/{student_id}")
def update_student(student_id: str, updated_data: StudentUpdate, db: Session = Depends(get_db)):

    student = db.query(models.Student).filter(
        models.Student.student_id == student_id
    ).first()

    if not student:
        return {"message": "Student not found"}

    for key, value in updated_data.dict(exclude_unset=True).items():
        setattr(student, key, value)

    db.commit()
    db.refresh(student)

    return student

# -----------------------------
# Delete Student by ID
# -----------------------------
@app.delete("/students/{student_id}")
def delete_student(student_id: str, db: Session = Depends(get_db)):
    student = crud.delete_student(db, student_id)

    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    return {"message": "Student deleted successfully"}

# # ------------------------
# # Get All Students From DB
# # ------------------------
# @app.get("/students", response_model=List[schema.StudentCreate])
# def get_all_students(db: Session = Depends(get_db)):
#     students = db.query(models.Student).all()
#     return students

# ------------------------
# Get Student by ID from DB
# ------------------------
@app.get("/students/{student_id_db}")
def get_student(student_id: str, db: Session = Depends(get_db)):
    student = db.query(models.Student).filter(
        models.Student.student_id == student_id
    ).first()

    if not student:
        return {"message": "Student not found"}

    return student
# --------------------------------
# Get Students by Greater than Age 
# --------------------------------
@app.get("/students/age-greater-than/{age}")
def get_students_age_greater(age: int):
    result = df[df["age"] > age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}
    
# -------------------------------
# Get Students by Lesser than Age 
# -------------------------------
@app.get("/students/age-lesser-than/{age}")
def get_students_age_lesser(age: int):
    result = df[df["age"] < age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}
    

# ---------------------------------------
# Get Students by Greater than Attendance
# ---------------------------------------
@app.get("/students/attendance-greater-than/{attendance}")
def get_students_attendance_greater(attendance: float):
    result = df[df["attendance"] > attendance]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}

# --------------------------------------
# Get Students by Lesser than Attendance
# --------------------------------------
@app.get("/students/attendance-lesser-than/{attendance}")
def get_students_attendance_lesser(attendance: float):
    result = df[df["attendance"] < attendance]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}


# -----------------------------------------------
# Get Students by Greater than Scholarship Amount
# -----------------------------------------------   
@app.get("/students/scholarship-greater-than/{scholarship}")
def get_students_scholarship_greater(scholarship: int):
    result = df[df["scholarship"] > scholarship]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}

# -----------------------------------------------
# Get Students by Lesser than Scholarship Amount
# -----------------------------------------------
@app.get("/students/scholarship-lesser-than/{scholarship}")
def get_students_scholarship_lesser(scholarship: int):
    result = df[df["scholarship"] < scholarship]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}


    
# -----------------------------
# Get Students by City  
# -----------------------------
@app.get("/students/city/{city}")
def get_students_by_city(city: str):
    result = df[df["city"].str.lower() == city.lower()]
    return result.to_dict(orient="records")


# -----------------------------
# Get Students by Status 
# -----------------------------
@app.get("/students/status/{status}")
def get_students_by_status(status: str):
    result = df[df["status"] == status]
    return result.to_dict(orient="records")


# -----------------------------
# Get Students by Major
# -----------------------------
@app.get("/students/major/{major}")
def get_students_by_major(major: str):
    result = df[df["major"] == major]
    return result.to_dict(orient="records")


# -----------------------------
# Age Greater Than X
# -----------------------------
@app.get("/students/age-greater-than/{age}")
def get_students_age_greater(age: int):
    result = df[df["age"] > age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}


# -----------------------------
# Age Less Than X
# -----------------------------
@app.get("/students/age-lesser-than/{age}")
def get_students_age_lesser(age: int):
    result = df[df["age"] < age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}


# -----------------------------
# Age Equal To X
# -----------------------------
@app.get("/students/age-equal/{age}")
def get_students_age_equal(age: int):
    result = df[df["age"] == age]

    if not result.empty:
        return result.to_dict(orient="records")
    else:
        return {"message": "No students found"}