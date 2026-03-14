from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "User Service"
    secret_key: str = "super-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    database_url: str = "postgresql+asyncpg://ns_user:ns_password@localhost:5432/user_db"
    redis_url: str = "redis://localhost:6379/1"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

