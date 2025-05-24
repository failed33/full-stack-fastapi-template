import uuid

from pydantic import BaseModel, Field


class BasePipelineEvent(BaseModel):
    """Base class for all pipeline events with common fields."""

    trace_id: uuid.UUID = Field(
        ..., description="Unique identifier to trace this workflow"
    )
    original_file_id: uuid.UUID = Field(
        ..., description="ID of the original file being processed"
    )
    user_id: uuid.UUID = Field(..., description="ID of the user who owns the file")
    target_hardware: str = Field(
        default="cpu", description="Target hardware for processing (cpu, cuda, rocm)"
    )


class FileReadyForConversionEvent(BasePipelineEvent):
    """Event triggered when a file is ready for conversion (after upload completion)."""

    minio_object_name: str = Field(
        ..., description="Path to the original file in MinIO"
    )
    original_filename: str = Field(
        ..., description="Original filename as uploaded by user"
    )


class ConversionCompletedEvent(BasePipelineEvent):
    """Event triggered when file conversion is completed."""

    converted_file_minio_path: str = Field(
        ..., description="Path to the converted file in MinIO"
    )
    converted_file_format: str = Field(
        ..., description="Format of the converted file (e.g., 'wav')"
    )


class SegmentCreatedEvent(BasePipelineEvent):
    """Event triggered when a file segment is created and ready for transcription."""

    parent_converted_file_minio_path: str = Field(
        ..., description="Path to the parent converted file"
    )
    segment_id: uuid.UUID = Field(
        ..., description="Database ID of the FileSegment record"
    )
    segment_minio_path: str = Field(
        ..., description="Path to the segment file in MinIO"
    )
    segment_index: int = Field(..., description="Index of this segment within the file")
    total_segments: int = Field(
        ..., description="Total number of segments for this file"
    )


class TranscriptionCompletedEvent(BasePipelineEvent):
    """Event triggered when transcription of a segment is completed."""

    segment_id: uuid.UUID = Field(
        ..., description="Database ID of the FileSegment record"
    )
    transcription_text_minio_path: str = Field(
        ..., description="Path to the transcription text file in MinIO"
    )
    transcription_summary: str = Field(
        ..., description="Brief summary of the transcription content"
    )
