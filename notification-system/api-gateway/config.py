from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "API Gateway"
    secret_key: str = "super-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    database_url: str = "postgresql+asyncpg://ns_user:ns_password@localhost:5432/notification_db"
    redis_url: str = "redis//localhost:6379/0"
    rabbitmq_url: str = "amqp://ns_rabbit:ns_rabbit_pass@localhost:5672/"

    user_service_url: str = "http://localhost:8001"
    template_service_url: str = "http://localhost:8004"

    # RabbitMQ topology
    exchange_name: str = "notification.direct"
    email_queue: str = "email.queue"
    push_queue: str = "push.queue"
    failed_queue: str = "failed.queue"

    # Rate limiting
    rate_limit_per_minute: int = 100

    class Config:
        env_file = "env"
        extra = "ignore"

settings = settings()