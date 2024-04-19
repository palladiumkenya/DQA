import logging
import os
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime

from database import get_db_source, get_db_dest
from models import RabbitMQMessage, DQADwapicentral, DQADwapiCentralPatientDuplicate

router = APIRouter()


# Read queries from external SQL files
def read_query_file(docket: str) -> str:
    file_path = os.path.join("Queries", f"{docket.upper()}.sql")
    with open(file_path, "r") as file:
        return file.read()


# Dynamic Query Configuration
INDICATOR_QUERIES = {
    'TX_CURR': read_query_file('TX_CURR'),
}


# API endpoint to handle RabbitMQ messages
@router.post("/process_dqa")
async def process_message(message: RabbitMQMessage, db_source: Session = Depends(get_db_source),
                          db_dest: Session = Depends(get_db_dest)):
    # Extract Docket and MFL Code from the message

    # Iterate over the indicators queries and execute them
    for indicator, query_source in INDICATOR_QUERIES.items():
        process_metrics_dqa(db_source, query_source, message, indicator, db_dest)

    process_duplicates_dqa(read_query_file('CheckDuplicatePatients'), db_source, message, db_dest)
    return {"message": "Processing complete"}


def process_metrics_dqa(db_source, query_source, message, indicator_name, db_dest):
    try:
        query_result_source = db_source.execute(text(query_source), {"mfl_code": message.MFL_Code}).fetchall()

        # Extract additional information from the RabbitMQ message
        facility_name = message.Facility
        query_value = str(query_result_source[0][0]) if query_result_source else None # Access the value from the result if available
        log_date = datetime.strptime(message.log_date, "%Y-%m-%dT%H:%M:%S.%f") # Replace with the actual log date
        dwapi_version = message.dwapi_version  # Replace with the actual version

        # Store the response in DqaDwapiCentral table in the destination database
        dqa_dwapi_central_entry = DQADwapicentral(
            mfl_code=message.MFL_Code,
            name=facility_name,
            indicator=indicator_name,
            value=query_value,
            log_date=log_date,
            dwapi_version=dwapi_version,
            docket=message.Docket
        )
        db_dest.add(dqa_dwapi_central_entry)
        db_dest.commit()

        logging.info(f"Query '{indicator_name}' executed successfully. Indicator Value: {query_value}")

    except Exception as e:
        # Log the error and continue to the next iteration
        logging.error(f"Error processing query '{indicator_name}': {str(e)}")

    return


def process_duplicates_dqa(query, db_source, message, db_dest):
    try:
        query_result_source = db_source.execute(text(query), {"mfl_code": message.MFL_Code}).fetchall()
        print(query_result_source)

        # Extract additional information from the RabbitMQ message
        facility_name = message.Facility
        query_value = str(query_result_source[0][0]) if query_result_source else 0
        log_date = datetime.strptime(message.log_date, "%Y-%m-%dT%H:%M:%S.%f")
        indicator_date = datetime.strptime(message.indicator_date, "%Y-%m-%d")

        dqa_dwapi_central_entry = DQADwapiCentralPatientDuplicate(
            mfl_code=message.MFL_Code,
            name=facility_name,
            number_of_dups=query_value,
            log_date=log_date,
            reporting_date=indicator_date,
        )
        db_dest.add(dqa_dwapi_central_entry)
        db_dest.commit()

        logging.info(f"DUPS query executed successfully for {facility_name}")

    except Exception as e:
        # Log the error and continue to the next iteration
        logging.error(f"Error processing DUPS query '{message.Facility}': {str(e)}")
    return
