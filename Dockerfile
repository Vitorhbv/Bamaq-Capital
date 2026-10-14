FROM python:3.13.3-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml alembic.ini ./
COPY src ./src
COPY migrations ./migrations
COPY tests ./tests
COPY scripts ./scripts
RUN pip install --no-cache-dir --no-deps --no-build-isolation -e . \
    && useradd --create-home --uid 10001 appuser
USER appuser
ENTRYPOINT ["bamaq"]
CMD ["api"]
