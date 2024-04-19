from pydantic import BaseSettings


class Settings(BaseSettings):
    Rabbit_MQ_username: str
    password: str
    host: str
    port: str
    virtual_host: str
    exchange_name: str
    queue_name: str
    route_key: str
    MS_SQL_SERVER: str
    MS_SQL_USERNAME: str
    MS_SQL_PASSWORD: str
    MS_SQL_DATABASE: str
    MS_SQL_SERVER_SOURCE: str
    MS_SQL_USERNAME_SOURCE: str
    MS_SQL_PASSWORD_SOURCE: str
    MS_SQL_DATABASE_SOURCE: str

    class Config:
        def __init__(self):
            pass

        env_file = './.env'


settings = Settings()
