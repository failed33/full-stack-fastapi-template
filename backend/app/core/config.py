import secrets
import warnings
from typing import Annotated, Any, Literal

from pydantic import (
    AnyUrl,
    BeforeValidator,
    EmailStr,
    HttpUrl,
    PostgresDsn,
    computed_field,
    model_validator,
)
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self


def parse_cors(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Use top level .env file (one level above ./backend/)
        env_file="../.env",
        env_ignore_empty=True,
        extra="ignore",
    )
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    FRONTEND_HOST: str = "http://localhost:5173"
    ENVIRONMENT: Literal["local", "staging", "production"] = "local"

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def all_cors_origins(self) -> list[str]:
        origins = [str(origin).rstrip("/") for origin in self.BACKEND_CORS_ORIGINS]
        if self.FRONTEND_HOST.rstrip("/") not in origins:
            origins.append(self.FRONTEND_HOST.rstrip("/"))
        if (
            self.FRONTEND_ORIGIN
            and str(self.FRONTEND_ORIGIN).rstrip("/") not in origins
        ):
            origins.append(str(self.FRONTEND_ORIGIN).rstrip("/"))
        return sorted(set(origins))

    PROJECT_NAME: str
    SENTRY_DSN: HttpUrl | None = None
    POSTGRES_SERVER: str
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> PostgresDsn:
        return MultiHostUrl.build(
            scheme="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_SERVER,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        )

    SMTP_TLS: bool = True
    SMTP_SSL: bool = False
    SMTP_PORT: int | None = None
    SMTP_HOST: str | None = None
    SMTP_USER: str | None = None
    SMTP_PASSWORD: str | None = None
    EMAILS_FROM_EMAIL: EmailStr | None = None
    EMAILS_FROM_NAME: str | None = None

    @model_validator(mode="after")
    def _set_default_emails_from_name(self) -> Self:
        if self.emails_enabled and not self.EMAILS_FROM_NAME:
            self.EMAILS_FROM_NAME = self.PROJECT_NAME
        return self

    EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = 48

    @computed_field  # type: ignore[prop-decorator]
    @property
    def emails_enabled(self) -> bool:
        return bool(self.SMTP_HOST and self.SMTP_PORT and self.EMAILS_FROM_EMAIL)

    EMAIL_TEST_USER: EmailStr = "test@example.com"
    FIRST_SUPERUSER: EmailStr
    FIRST_SUPERUSER_PASSWORD: str

    # minIO configuration parameters
    MINIO_URL_INTERNAL: str = "minio:9000"
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_PRIMARY_BUCKET: str = "uploads"
    MINIO_SECONDARY_BUCKET: str = "uploads.segments"
    MINIO_TERTIARY_BUCKET: str = "uploads.transcripts"
    FRONTEND_ORIGIN: HttpUrl | None = None
    MINIO_KAFKA_NOTIFICATION_ARN: str | None = None

    # dramatiq configuration parameters
    DRAMATIQ_GROUP_ID: str = "tts-workers"
    DRAMATIQ_MAX_POLL_RECORDS: int = 32
    DRAMATIQ_AUTO_OFFSET_RESET: str = "earliest"

    # kafka configuration parameters
    KAFKA_BOOTSTRAP: str = "kafka:9092"
    KAFKA_MAX_POLL_RECORDS: int = 32
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    MINIO_NOTIFY_KAFKA_TOPIC_PRIMARY: str = "minio.uploads"
    MINIO_NOTIFY_KAFKA_TOPIC_SECONDARY: str = "minio.uploads.segments"
    MINIO_NOTIFY_KAFKA_TOPIC_TERTIARY: str = "minio.uploads.transcripts"

    @model_validator(mode="after")
    def _set_default_frontend_origin(self) -> Self:
        if self.FRONTEND_ORIGIN is None:
            try:
                self.FRONTEND_ORIGIN = HttpUrl(self.FRONTEND_HOST)
            except Exception as e:
                warnings.warn(
                    f"Could not parse FRONTEND_HOST ('{self.FRONTEND_HOST}') as HttpUrl for FRONTEND_ORIGIN: {e}",
                    stacklevel=2,
                )
                self.FRONTEND_ORIGIN = None
        return self

    def _check_default_secret(self, var_name: str, value: str | None) -> None:
        if value == "changethis":
            message = (
                f'The value of {var_name} is "changethis", '
                "for security, please change it, at least for deployments."
            )
            if self.ENVIRONMENT == "local":
                warnings.warn(message, stacklevel=1)
            else:
                raise ValueError(message)

    @model_validator(mode="after")
    def _enforce_non_default_secrets(self) -> Self:
        self._check_default_secret("SECRET_KEY", self.SECRET_KEY)
        self._check_default_secret("POSTGRES_PASSWORD", self.POSTGRES_PASSWORD)
        self._check_default_secret(
            "FIRST_SUPERUSER_PASSWORD", self.FIRST_SUPERUSER_PASSWORD
        )

        return self


settings = Settings()  # type: ignore
