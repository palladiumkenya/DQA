import logging
import os
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime

from database import get_db_source, get_db_dest
from models import RabbitMQMessage, DQADwapicentral

router = APIRouter()


# Read queries from external SQL files
def read_query_file(docket: str) -> str:
    file_path = os.path.join("Queries", f"{docket.upper()}.sql")
    with open(file_path, "r") as file:
        return file.read()


# Dynamic Query Configuration
TASK_QUERIES = {
    'TX_CURR': read_query_file('TX_CURR'),

    # Add more task types and their queries as needed
}


# API endpoint to handle RabbitMQ messages
@router.post("/process_dqa")
async def process_message(message: RabbitMQMessage, db_source: Session = Depends(get_db_source),
                          db_dest: Session = Depends(get_db_dest)):
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
            log_date = datetime.strptime(message.log_date, "%Y-%m-%dT%H:%M:%S.%f")  # Replace with the actual log date
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

    # Return statement outside the loop
    return {"message": "Processing complete"}

