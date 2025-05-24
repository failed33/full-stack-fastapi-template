import uuid
from datetime import datetime
from enum import Enum

from pydantic import EmailStr
from sqlalchemy import JSON as SAJson
from sqlalchemy import Column, func
from sqlalchemy import DateTime as SADateTime
from sqlalchemy import Text as SAText
from sqlmodel import Field, Relationship, SQLModel


# Helper Enums
class ProcessStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProcessType(str, Enum):
    TRANSCRIPTION = "transcription"
    ANALYSIS = "analysis"
    UNKNOWN = "unknown"


# Shared properties
class UserBase(SQLModel):
    email: EmailStr = Field(unique=True, index=True, max_length=255)
    is_active: bool = True
    is_superuser: bool = False
    full_name: str | None = Field(default=None, max_length=255)


# Properties to receive via API on creation
class UserCreate(UserBase):
    password: str = Field(min_length=8, max_length=40)


class UserRegister(SQLModel):
    email: EmailStr = Field(max_length=255)
    password: str = Field(min_length=8, max_length=40)
    full_name: str | None = Field(default=None, max_length=255)


# Properties to receive via API on update, all are optional
class UserUpdate(UserBase):
    email: EmailStr | None = Field(default=None, max_length=255)  # type: ignore
    password: str | None = Field(default=None, min_length=8, max_length=40)


class UserUpdateMe(SQLModel):
    full_name: str | None = Field(default=None, max_length=255)
    email: EmailStr | None = Field(default=None, max_length=255)


class UpdatePassword(SQLModel):
    current_password: str = Field(min_length=8, max_length=40)
    new_password: str = Field(min_length=8, max_length=40)


# Database model, database table inferred from class name
class User(UserBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    hashed_password: str
    items: list["Item"] = Relationship(back_populates="owner", cascade_delete=True)

    # --- New relationships for File and FileProcess ---
    files: list["File"] = Relationship(back_populates="user", cascade_delete=True)
    file_processes: list["FileProcess"] = Relationship(
        back_populates="user", cascade_delete=True
    )


# Properties to return via API, id is always required
class UserPublic(UserBase):
    id: uuid.UUID


class UsersPublic(SQLModel):
    data: list[UserPublic]
    count: int


# Shared properties
class ItemBase(SQLModel):
    title: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=255)


# Properties to receive on item creation
class ItemCreate(ItemBase):
    pass


# Properties to receive on item update
class ItemUpdate(ItemBase):
    title: str | None = Field(default=None, min_length=1, max_length=255)  # type: ignore


# Database model, database table inferred from class name
class Item(ItemBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    owner_id: uuid.UUID = Field(
        foreign_key="user.id", nullable=False
    )  # cascade_delete is on User.items
    owner: "User" = Relationship(back_populates="items")  # Item has one User (owner)


# Properties to return via API, id is always required
class ItemPublic(ItemBase):
    id: uuid.UUID
    owner_id: uuid.UUID


class ItemsPublic(SQLModel):
    data: list[ItemPublic]
    count: int


# Generic message
class Message(SQLModel):
    message: str


# JSON payload containing access token
class Token(SQLModel):
    access_token: str
    token_type: str = "bearer"


# Contents of JWT token
class TokenPayload(SQLModel):
    sub: str | None = None


class NewPassword(SQLModel):
    token: str
    new_password: str = Field(min_length=8, max_length=40)


# --- New Model: File ---
class FileBase(SQLModel):
    original_filename: str = Field(max_length=255, index=True)
    minio_object_name: str = Field(max_length=1024, unique=True, index=True)
    file_type: str | None = Field(default=None, max_length=100)
    size_bytes: int | None = Field(default=None)
    upload_status: ProcessStatus = Field(
        default=ProcessStatus.PENDING, max_length=50, index=True
    )
    s3_etag: str | None = Field(default=None, max_length=255)

    upload_started_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(
            SADateTime(timezone=True), server_default=func.now(), nullable=False
        ),
    )
    upload_completed_at: datetime | None = Field(
        default=None, sa_column=Column(SADateTime(timezone=True), nullable=True)
    )


class File(FileBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True, index=True)
    user_id: uuid.UUID = Field(foreign_key="user.id", index=True, nullable=False)

    user: "User" = Relationship(back_populates="files")
    processes: list["FileProcess"] = Relationship(
        back_populates="file", cascade_delete=True
    )


class FileCreate(FileBase):
    user_id: uuid.UUID  # Required when creating a File record directly


class FilePublic(FileBase):
    id: uuid.UUID
    user_id: uuid.UUID


class FilesPublic(SQLModel):
    data: list[FilePublic]
    count: int


# --- New Model: FileProcess ---
class FileProcessBase(SQLModel):
    process_type: ProcessType = Field(
        default=ProcessType.UNKNOWN, max_length=100, index=True
    )
    status: ProcessStatus = Field(
        default=ProcessStatus.PENDING, max_length=50, index=True
    )

    initiated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(
            SADateTime(timezone=True), server_default=func.now(), nullable=False
        ),
    )
    completed_at: datetime | None = Field(
        default=None, sa_column=Column(SADateTime(timezone=True), nullable=True)
    )
    duration_ms: int | None = Field(default=None)
    error_message: str | None = Field(
        default=None, sa_column=Column(SAText(), nullable=True)
    )
    result_data: dict | None = Field(
        default=None, sa_column=Column(SAJson(), nullable=True)
    )


class FileProcess(FileProcessBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True, index=True)
    file_id: uuid.UUID = Field(foreign_key="file.id", index=True, nullable=False)
    user_id: uuid.UUID = Field(foreign_key="user.id", index=True, nullable=False)

    file: "File" = Relationship(back_populates="processes")
    user: "User" = Relationship(back_populates="file_processes")
    segments: list["FileSegment"] = Relationship(
        back_populates="file_process", cascade_delete=True
    )


class FileProcessCreate(FileProcessBase):
    file_id: uuid.UUID
    user_id: uuid.UUID  # Ensure user_id is present for creation


class FileProcessPublic(FileProcessBase):
    id: uuid.UUID
    file_id: uuid.UUID
    user_id: uuid.UUID


class FileProcessesPublic(SQLModel):
    data: list[FileProcessPublic]
    count: int


# --- New Model: FileSegment ---
class FileSegmentBase(SQLModel):
    segment_index: int = Field(index=True)
    minio_object_name: str = Field(
        max_length=1024, unique=True, index=True
    )  # Path to the segment in MinIO
    status: ProcessStatus = Field(
        default=ProcessStatus.PENDING, max_length=50, index=True
    )

    transcription_text_minio_path: str | None = Field(
        default=None, max_length=1024
    )  # Path to transcription text file
    transcription_summary: str | None = Field(
        default=None, sa_column=Column(SAText(), nullable=True)
    )  # A brief summary

    processing_started_at: datetime | None = Field(
        default=None, sa_column=Column(SADateTime(timezone=True), nullable=True)
    )
    processing_completed_at: datetime | None = Field(
        default=None, sa_column=Column(SADateTime(timezone=True), nullable=True)
    )
    processing_duration_ms: int | None = Field(default=None)
    error_message: str | None = Field(
        default=None, sa_column=Column(SAText(), nullable=True)
    )


class FileSegment(FileSegmentBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True, index=True)

    file_process_id: uuid.UUID = Field(
        foreign_key="fileprocess.id", index=True, nullable=False
    )
    original_file_id: uuid.UUID = Field(
        foreign_key="file.id", index=True, nullable=False
    )  # Denormalized for convenience
    user_id: uuid.UUID = Field(
        foreign_key="user.id", index=True, nullable=False
    )  # Denormalized for convenience

    file_process: "FileProcess" = Relationship(back_populates="segments")
    original_file: "File" = (
        Relationship()
    )  # No back_populates needed if only linking one way for query
    user: "User" = (
        Relationship()
    )  # No back_populates needed if only linking one way for query


class FileSegmentCreate(FileSegmentBase):
    file_process_id: uuid.UUID
    original_file_id: uuid.UUID
    user_id: uuid.UUID


class FileSegmentPublic(FileSegmentBase):
    id: uuid.UUID
    file_process_id: uuid.UUID
    original_file_id: uuid.UUID
    user_id: uuid.UUID


class FileSegmentsPublic(SQLModel):
    data: list[FileSegmentPublic]
    count: int
