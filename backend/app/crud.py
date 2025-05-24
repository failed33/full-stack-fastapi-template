import uuid
from typing import Any

from sqlmodel import Session, select

from app.core.security import get_password_hash, verify_password
from app.models import (
    File,
    FileCreate,
    FileProcess,
    FileProcessCreate,
    FileSegment,
    FileSegmentCreate,
    Item,
    ItemCreate,
    User,
    UserCreate,
    UserUpdate,
)


def create_user(*, session: Session, user_create: UserCreate) -> User:
    db_obj = User.model_validate(
        user_create, update={"hashed_password": get_password_hash(user_create.password)}
    )
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def update_user(*, session: Session, db_user: User, user_in: UserUpdate) -> Any:
    user_data = user_in.model_dump(exclude_unset=True)
    extra_data = {}
    if "password" in user_data:
        password = user_data["password"]
        hashed_password = get_password_hash(password)
        extra_data["hashed_password"] = hashed_password
    db_user.sqlmodel_update(user_data, update=extra_data)
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    return db_user


def get_user_by_email(*, session: Session, email: str) -> User | None:
    statement = select(User).where(User.email == email)
    session_user = session.exec(statement).first()
    return session_user


def authenticate(*, session: Session, email: str, password: str) -> User | None:
    db_user = get_user_by_email(session=session, email=email)
    if not db_user:
        return None
    if not verify_password(password, db_user.hashed_password):
        return None
    return db_user


def create_item(*, session: Session, item_in: ItemCreate, owner_id: uuid.UUID) -> Item:
    db_item = Item.model_validate(item_in, update={"owner_id": owner_id})
    session.add(db_item)
    session.commit()
    session.refresh(db_item)
    return db_item


# --- CRUD for File ---


def create_file(*, session: Session, file_in: FileCreate, user_id: uuid.UUID) -> File:
    """Create a new file record in the database."""
    # Set the user_id from the authenticated user for security
    db_file = File.model_validate(file_in, update={"user_id": user_id})
    session.add(db_file)
    session.commit()
    session.refresh(db_file)
    return db_file


def get_file(*, session: Session, file_id: uuid.UUID) -> File | None:
    """Retrieve a file by its ID."""
    statement = select(File).where(File.id == file_id)
    db_file = session.exec(statement).first()
    return db_file


def get_file_by_minio_object_name(
    *, session: Session, minio_object_name: str
) -> File | None:
    """Retrieve a file by its MinIO object name."""
    statement = select(File).where(File.minio_object_name == minio_object_name)
    db_file = session.exec(statement).first()
    return db_file


def get_files_by_user(
    *, session: Session, user_id: uuid.UUID, skip: int = 0, limit: int = 100
) -> list[File]:
    """Retrieve all files for a specific user, with pagination."""
    statement = (
        select(File)
        .where(File.user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(File.upload_started_at.desc())
    )
    files = session.exec(statement).all()
    return files


def update_file_upload_completed(
    *, session: Session, file_id: uuid.UUID, size_bytes: int
) -> File | None:
    """Update file record when upload is completed."""
    from datetime import datetime

    db_file = get_file(session=session, file_id=file_id)
    if db_file:
        db_file.upload_completed_at = datetime.utcnow()
        db_file.size_bytes = size_bytes
        session.add(db_file)
        session.commit()
        session.refresh(db_file)
    return db_file


def update_file_upload_completed_with_metadata(
    *, session: Session, file_id: uuid.UUID, size_bytes: int, s3_etag: str
) -> File | None:
    """Update file record when upload is completed with full S3 metadata."""
    from datetime import datetime, timezone

    from app.models import ProcessStatus

    db_file = get_file(session=session, file_id=file_id)
    if db_file:
        db_file.upload_completed_at = datetime.now(timezone.utc)
        db_file.size_bytes = size_bytes
        db_file.s3_etag = s3_etag
        db_file.upload_status = ProcessStatus.COMPLETED
        session.add(db_file)
        session.commit()
        session.refresh(db_file)
    return db_file


def delete_file(*, session: Session, file_id: uuid.UUID) -> File | None:
    """Delete a file record by its ID."""
    db_file = get_file(session=session, file_id=file_id)
    if db_file:
        session.delete(db_file)
        session.commit()
    return db_file


# --- CRUD for FileProcess ---


def create_file_process(
    *, session: Session, process_in: FileProcessCreate
) -> FileProcess:
    """Create a new file process record in the database."""
    db_process = FileProcess.model_validate(process_in)
    session.add(db_process)
    session.commit()
    session.refresh(db_process)
    return db_process


def get_file_process(*, session: Session, process_id: uuid.UUID) -> FileProcess | None:
    """Retrieve a file process by its ID."""
    statement = select(FileProcess).where(FileProcess.id == process_id)
    db_process = session.exec(statement).first()
    return db_process


def get_file_processes_by_file(
    *, session: Session, file_id: uuid.UUID, skip: int = 0, limit: int = 100
) -> list[FileProcess]:
    """Retrieve all processes for a specific file, with pagination."""
    statement = (
        select(FileProcess)
        .where(FileProcess.file_id == file_id)
        .offset(skip)
        .limit(limit)
        .order_by(FileProcess.initiated_at.desc())
    )
    processes = session.exec(statement).all()
    return processes


def get_file_processes_by_user(
    *, session: Session, user_id: uuid.UUID, skip: int = 0, limit: int = 100
) -> list[FileProcess]:
    """Retrieve all processes for a specific user, with pagination."""
    statement = (
        select(FileProcess)
        .where(FileProcess.user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(FileProcess.initiated_at.desc())
    )
    processes = session.exec(statement).all()
    return processes


def update_file_process_status(
    *,
    session: Session,
    process_id: uuid.UUID,
    status: str,
    error_message: str | None = None,
) -> FileProcess | None:
    """Update the status of a file process."""
    from datetime import datetime

    from app.models import ProcessStatus

    db_process = get_file_process(session=session, process_id=process_id)
    if db_process:
        db_process.status = ProcessStatus(status)
        if error_message:
            db_process.error_message = error_message
        if status in [
            ProcessStatus.COMPLETED,
            ProcessStatus.FAILED,
            ProcessStatus.CANCELLED,
        ]:
            db_process.completed_at = datetime.utcnow()
            if db_process.initiated_at and db_process.completed_at:
                duration = db_process.completed_at - db_process.initiated_at
                db_process.duration_ms = int(duration.total_seconds() * 1000)
        session.add(db_process)
        session.commit()
        session.refresh(db_process)
    return db_process


def update_file_process_result(
    *, session: Session, process_id: uuid.UUID, result_data: dict
) -> FileProcess | None:
    """Update the result data of a file process."""
    db_process = get_file_process(session=session, process_id=process_id)
    if db_process:
        db_process.result_data = result_data
        session.add(db_process)
        session.commit()
        session.refresh(db_process)
    return db_process


def delete_file_process(
    *, session: Session, process_id: uuid.UUID
) -> FileProcess | None:
    """Delete a file process record by its ID."""
    db_process = get_file_process(session=session, process_id=process_id)
    if db_process:
        session.delete(db_process)
        session.commit()
    return db_process


# --- CRUD for FileSegment ---


def create_file_segment(
    *, session: Session, segment_in: "FileSegmentCreate"
) -> "FileSegment":
    """Create a new file segment record in the database."""
    from app.models import FileSegment

    db_segment = FileSegment.model_validate(segment_in)
    session.add(db_segment)
    session.commit()
    session.refresh(db_segment)
    return db_segment


def get_file_segment(
    *, session: Session, segment_id: uuid.UUID
) -> "FileSegment | None":
    """Retrieve a file segment by its ID."""
    from app.models import FileSegment

    statement = select(FileSegment).where(FileSegment.id == segment_id)
    db_segment = session.exec(statement).first()
    return db_segment


def get_file_segments_by_process(
    *, session: Session, process_id: uuid.UUID, skip: int = 0, limit: int = 100
) -> list["FileSegment"]:
    """Retrieve all segments for a specific file process, with pagination."""
    from app.models import FileSegment

    statement = (
        select(FileSegment)
        .where(FileSegment.file_process_id == process_id)
        .offset(skip)
        .limit(limit)
        .order_by(FileSegment.segment_index.asc())
    )
    segments = session.exec(statement).all()
    return segments


def get_file_segments_by_user(
    *, session: Session, user_id: uuid.UUID, skip: int = 0, limit: int = 100
) -> list["FileSegment"]:
    """Retrieve all segments for a specific user, with pagination."""
    from app.models import FileSegment

    statement = (
        select(FileSegment)
        .where(FileSegment.user_id == user_id)
        .offset(skip)
        .limit(limit)
        .order_by(FileSegment.processing_started_at.desc())
    )
    segments = session.exec(statement).all()
    return segments


def update_file_segment_status(
    *,
    session: Session,
    segment_id: uuid.UUID,
    status: str,
    error_message: str | None = None,
) -> "FileSegment | None":
    """Update the status of a file segment."""
    from datetime import datetime

    from app.models import ProcessStatus

    db_segment = get_file_segment(session=session, segment_id=segment_id)
    if db_segment:
        db_segment.status = ProcessStatus(status)
        if error_message:
            db_segment.error_message = error_message

        if status == ProcessStatus.PROCESSING and not db_segment.processing_started_at:
            db_segment.processing_started_at = datetime.utcnow()
        elif status in [
            ProcessStatus.COMPLETED,
            ProcessStatus.FAILED,
            ProcessStatus.CANCELLED,
        ]:
            db_segment.processing_completed_at = datetime.utcnow()
            if db_segment.processing_started_at and db_segment.processing_completed_at:
                duration = (
                    db_segment.processing_completed_at
                    - db_segment.processing_started_at
                )
                db_segment.processing_duration_ms = int(duration.total_seconds() * 1000)

        session.add(db_segment)
        session.commit()
        session.refresh(db_segment)
    return db_segment


def update_file_segment_transcription_result(
    *,
    session: Session,
    segment_id: uuid.UUID,
    status: str,
    result_path: str | None = None,
    summary: str | None = None,
) -> "FileSegment | None":
    """Update the transcription result data of a file segment."""
    db_segment = get_file_segment(session=session, segment_id=segment_id)
    if db_segment:
        if result_path:
            db_segment.transcription_text_minio_path = result_path
        if summary:
            db_segment.transcription_summary = summary
        # Also update status
        return update_file_segment_status(
            session=session, segment_id=segment_id, status=status
        )
    return db_segment


def delete_file_segment(
    *, session: Session, segment_id: uuid.UUID
) -> "FileSegment | None":
    """Delete a file segment record by its ID."""
    db_segment = get_file_segment(session=session, segment_id=segment_id)
    if db_segment:
        session.delete(db_segment)
        session.commit()
    return db_segment
