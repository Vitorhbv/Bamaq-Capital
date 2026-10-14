from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BAMAQ_", env_file=".env", extra="ignore")

    database_url: str = "mysql+pymysql://bamaq:bamaq@localhost:3306/bamaq?charset=utf8mb4"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "bamaq-job-ingestion-v1"
    risk_url: str = "http://localhost:8001"
    risk_timeout_seconds: float = Field(default=5, gt=0, le=5)
    max_attempts: int = Field(default=16, ge=1, le=100)
    lease_seconds: int = Field(default=30, ge=10)
    poll_seconds: float = Field(default=0.5, gt=0, le=10)
    log_level: str = "INFO"
    mock_mode: str = "normal"
    mock_delay_seconds: float = Field(default=0, ge=0, le=60)
    mock_fail_first: int = Field(default=0, ge=0)
    mock_approval_limit: str = "5000.00"
