# main.py
import json
import logging
import os
import asyncio
import aio_pika
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from datetime import datetime

# Import Settings from settings.py
from settings import settings

# FastAPI app
app = FastAPI()

# SQLAlchemy configuration for destination database
DATABASE_URL_DEST = (
    f'mssql+pyodbc://{settings.MS_SQL_USERNAME}:{settings.MS_SQL_PASSWORD}'
    f'@{settings.MS_SQL_SERVER}/{settings.MS_SQL_DATABASE}'
)

# SQLAlchemy configuration for source database
DATABASE_URL_SOURCE = (
    f'mssql+pyodbc://{settings.MS_SQL_USERNAME_SOURCE}:{settings.MS_SQL_PASSWORD_SOURCE}'
    f'@{settings.MS_SQL_SERVER_SOURCE}/{settings.MS_SQL_DATABASE_SOURCE}'
)

engine_dest = create_engine(DATABASE_URL_DEST)
engine_source = create_engine(DATABASE_URL_SOURCE)

SessionLocalDest = sessionmaker(autocommit=False, autoflush=False, bind=engine_dest)
SessionLocalSource = sessionmaker(autocommit=False, autoflush=False, bind=engine_source)

Base = declarative_base()

# Define dqadwapicentral table
class DQADwapicentral(Base):
    __tablename__ = "dqadwapicentral"
    id = Column(Integer, primary_key=True, index=True)
    mfl_code = Column(Integer, index=True)
    name = Column(String)  # Facility Name
    indicator = Column(String)  # Name of the query
    value = Column(String)  # Query results
    log_date = Column(DateTime, default=datetime.utcnow)
    dwapi_version = Column(String)  # Dwapi Version
    docket = Column(String)

# Create the table in the destination database
Base.metadata.create_all(bind=engine_dest)

# Pydantic model for RabbitMQ message
class RabbitMQMessage(BaseModel):
    Facility: str
    MFL_Code: int
    Docket: str
    indicator_date: str
    Message: str
    log_date: str
    dwapi_version: str

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

# Read queries from external SQL files
def read_query_file(docket: str) -> str:
    file_path = os.path.join("queries", f"{docket.upper()}.sql")
    with open(file_path, "r") as file:
        return file.read()

# Dynamic Query Configuration

# Dynamic Query Configuration
TASK_QUERIES = {
    'TX_CURR': read_query_file('TX_CURR'),

    # Add more task types and their queries as needed
}

# Listen to RabbitMQ queue
async def listen_to_queue():
    connection = await aio_pika.connect_robust(
        f"amqp://{settings.Rabbit_MQ_username}:{settings.password}@{settings.host}:{settings.port}/{settings.virtual_host}"
    )

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    exchange_name = settings.exchange_name
    queue_name = settings.queue_name
    route_key = settings.route_key

    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.DIRECT
    )

    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(exchange, route_key)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                print("Received message:", message.body)

                # Deserialize the message body into a RabbitMQMessage object
                decoded_body = message.body.decode('utf-8')

                # Replace single quotes with double quotes in the payload
                corrected_body = decoded_body.replace("'", "\"")

                # Parse the payload as a JSON object
                rabbitmq_message_dict = json.loads(corrected_body)

                # Dynamically update 'MFL Code' to 'MFL_Code' if it exists
                if 'MFL Code' in rabbitmq_message_dict:
                    rabbitmq_message_dict['MFL_Code'] = rabbitmq_message_dict.pop('MFL Code')

                # Create an instance of RabbitMQMessage
                rabbitmq_message = RabbitMQMessage(**rabbitmq_message_dict)

                # Call process_message with the deserialized message
                await process_message(rabbitmq_message, db_source=SessionLocalSource(), db_dest=SessionLocalDest())


# Run the listener in the background
@app.on_event("startup")
async def startup_event(db_source: Session = Depends(get_db_source), db_dest: Session = Depends(get_db_dest)):
    asyncio.create_task(listen_to_queue())

# API endpoint to handle RabbitMQ messages
@app.post("/process_message")
async def process_message(message: RabbitMQMessage, db_source: Session = Depends(get_db_source), db_dest: Session = Depends(get_db_dest)):
    # Extract Docket and MFL Code from the message
    mfl_code = message.MFL_Code  # Extracted directly from the RabbitMQ message

    # Iterate over the task queries and execute them
    for indicator, query_source in TASK_QUERIES.items():
        try:
            query_result_source = db_source.execute(text(query_source), {"mfl_code": mfl_code}).fetchall()

            # Extract additional information from the RabbitMQ message
            facility_name = message.Facility
            indicator_name = indicator  # Use the indicator as the name of the query
            query_value = str(query_result_source[0][0]) if query_result_source else None  # Access the value from the result if available
            log_date = datetime.strptime(message.log_date, "%Y-%m-%dT%H:%M:%S.%f") # Replace with the actual log date
            dwapi_version = message.dwapi_version  # Replace with the actual version

            # Store the response in dqadwapicentral table in the destination database
            dqadwapicentral_entry = DQADwapicentral(
                mfl_code=mfl_code,
                name=facility_name,
                indicator=indicator_name,
                value=query_value,
                log_date=log_date,
                dwapi_version=dwapi_version,
                docket=message.Docket
            )
            db_dest.add(dqadwapicentral_entry)
            db_dest.commit()

            logging.info(f"Query '{indicator}' executed successfully. Indicator Value: {query_value}")

        except Exception as e:
            # Log the error and continue to the next iteration
            logging.error(f"Error processing query '{indicator}': {str(e)}")
            continue

    # Return statement outside of the loop
    return {"message": "Processing complete"}
