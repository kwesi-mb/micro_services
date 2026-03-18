from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str ="Template Service"
    database_url: str = "postgresql+asyncpg://ns_user:ns_password@localhost:5432/template_db"
    redis_url: str = "redis://localhost:6379/4"
    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()