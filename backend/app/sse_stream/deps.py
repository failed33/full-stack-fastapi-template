import logging
from collections.abc import Generator
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, Query, status
from jwt.exceptions import InvalidTokenError as JwtInvalidTokenError
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.core import security
from app.core.config import settings
from app.core.db import engine
from app.models import TokenPayload, User

logger = logging.getLogger(__name__)


# Database session dependency for HTTP requests
def get_http_db_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


HttpDbSessionDep = Annotated[Session, Depends(get_http_db_session)]


async def get_current_user_http(
    session: HttpDbSessionDep,
    token: str | None = Query(None, description="JWT token for authentication"),
) -> User:
    if not token:
        logger.warning("HTTP connection attempt without token for SSE.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated: Missing token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[security.ALGORITHM]
        )
        token_data = TokenPayload(**payload)
    except (JwtInvalidTokenError, ValidationError) as e:
        logger.warning(
            f"HTTP authentication error for SSE: Invalid token. Details: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    user = session.get(User, token_data.sub)
    if not user:
        logger.warning(
            f"HTTP authentication error for SSE: User not found (ID: {token_data.sub})."
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        logger.warning(
            f"HTTP authentication error for SSE: User inactive (ID: {token_data.sub})."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user"
        )

    logger.info(
        f"HTTP request for SSE successfully authenticated for user: {user.email} (ID: {user.id})"
    )
    return user


CurrentUserHttp = Annotated[User, Depends(get_current_user_http)]
