from pydantic import BaseModel, Field


# --- Define MinIO Event Schema using Pydantic Models ---
class MinioEventUserIdentity(BaseModel):
    principalId: str


class MinioEventRequestParameters(BaseModel):
    sourceIPAddress: str


class MinioEventResponseElements(BaseModel):
    x_amz_request_id: str = Field(..., alias="x-amz-request-id")
    x_minio_deployment_id: str | None = Field(None, alias="x-minio-deployment-id")
    x_minio_origin_endpoint: str | None = Field(None, alias="x-minio-origin-endpoint")


class MinioEventBucket(BaseModel):
    name: str
    ownerIdentity: MinioEventUserIdentity
    arn: str


class MinioEventObjectData(BaseModel):
    key: str
    size: int
    eTag: str | None = None
    contentType: str | None = None
    userMetadata: dict[str, str] | None = None
    sequencer: str | None = None


class MinioEventS3(BaseModel):
    s3SchemaVersion: str
    configurationId: str
    bucket: MinioEventBucket
    object: MinioEventObjectData


class MinioEventRecord(BaseModel):
    eventVersion: str
    eventSource: str
    awsRegion: str | None = None
    eventTime: str
    eventName: str
    userIdentity: MinioEventUserIdentity
    requestParameters: MinioEventRequestParameters
    responseElements: MinioEventResponseElements
    s3: MinioEventS3


class MinioNotification(BaseModel):
    Records: list[MinioEventRecord]


# --- End of Pydantic Models ---
