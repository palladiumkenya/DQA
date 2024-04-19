import asyncio
import aio_pika
import json

from models import RabbitMQMessage
from router.process_dqa import process_message
from settings import settings
from database import SessionLocalSource, SessionLocalDest


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
