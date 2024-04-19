from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import Settings from settings.py
from settings import settings

# SQLAlchemy configuration for destination database
DATABASE_URL_DEST = (
    f'mssql+pymssql://{settings.MS_SQL_USERNAME}:{settings.MS_SQL_PASSWORD}'
    f'@{settings.MS_SQL_SERVER}/{settings.MS_SQL_DATABASE}'
)

# SQLAlchemy configuration for source database
DATABASE_URL_SOURCE = (
    f'mssql+pymssql://{settings.MS_SQL_USERNAME_SOURCE}:{settings.MS_SQL_PASSWORD_SOURCE}'
    f'@{settings.MS_SQL_SERVER_SOURCE}/{settings.MS_SQL_DATABASE_SOURCE}'
)

engine_dest = create_engine(DATABASE_URL_DEST)
engine_source = create_engine(DATABASE_URL_SOURCE)

SessionLocalDest = sessionmaker(autocommit=False, autoflush=False, bind=engine_dest)
SessionLocalSource = sessionmaker(autocommit=False, autoflush=False, bind=engine_source)


# Dependency to get the database session for destination database
def get_db_dest():
    db = SessionLocalDest()
    try:
        yield db
    finally:
        db.close()


# Dependency to get the database session for source database
def get_db_source():
    db = SessionLocalSource()
    try:
        yield db
    finally:
        db.close()
