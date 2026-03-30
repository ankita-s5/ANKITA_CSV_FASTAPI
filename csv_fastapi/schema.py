from pydantic import BaseModel
from typing import Optional

class StudentCreate(BaseModel):
    student_id: str
    first_name: str
    last_name: str
    age: int
    major: str
    gpa: float
    attendance: float
    scholarship: int
    city: str
    status: str


class StudentUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    age: Optional[int] = None
    major: Optional[str] = None
    gpa: Optional[float] = None
    attendance: Optional[float] = None
    scholarship: Optional[int] = None
    city: Optional[str] = None
    status: Optional[str] = None

    class Config:
        from_attributes = True