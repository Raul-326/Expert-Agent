from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Expert Agent API"
    API_V1_STR: str = "/api/v1"
    BACKEND_CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000", "*"]
    
    # Celery & Redis
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    # Database
    DATABASE_URL: str = "sqlite:///../metrics_panel.db"

    # API Keys
    ARK_API_KEY: str = ""
    FEISHU_APP_ID: str = ""
    FEISHU_APP_SECRET: str = ""

    class Config:
        env_file = ".env"

settings = Settings()
