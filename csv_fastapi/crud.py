from sqlalchemy.orm import Session
import models


# -----------------------------
# CREATE
# -----------------------------
def create_student(db: Session, student_data: dict):
    student = models.Student(**student_data)
    db.add(student)
    db.commit()
    db.refresh(student)
    return student


# -----------------------------
# GET ALL
# -----------------------------
def get_all_students(db: Session):
    return db.query(models.Student).all()


# -----------------------------
# GET BY ID
# -----------------------------
def get_student_by_id(db: Session, student_id: str):
    return db.query(models.Student).filter(
        models.Student.student_id == student_id
    ).first()


# -----------------------------
# UPDATE
# -----------------------------
def update_student(db: Session, student_id: str, updated_data: dict):
    student = db.query(models.Student).filter(
        models.Student.student_id == student_id
    ).first()

    if not student:
        return None

    for key, value in updated_data.items():
        setattr(student, key, value)

    db.commit()
    db.refresh(student)
    return student


# -----------------------------
# DELETE
# -----------------------------
def delete_student(db: Session, student_id: str):
    student = db.query(models.Student).filter(
        models.Student.student_id == student_id
    ).first()

    if not student:
        return None

    db.delete(student)
    db.commit()
    return student