import azure.functions as func
import logging
import asyncio
from rstream import Producer

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

HOST="localhost"
USERNAME="guest"
PASSWORD="guest"
STREAM_NAME = "hello-python-stream"
STREAM_RETENTION = 5000000000 # 5GB

@app.route(route="sender")
async def sender(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    message = ""
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(f"Could not get payload from message body!", status_code=400)
    else:
        message = req_body.get('msg')

    async with Producer(
        host=HOST,
        username=USERNAME,
        password=PASSWORD,
    ) as producer:
        await producer.create_stream(STREAM_NAME, exists_ok=True, arguments={"MaxLengthBytes": STREAM_RETENTION})
        
        # converting to bytes for the stream
        message_bytes = message.encode("utf-8")
        await producer.send(stream=STREAM_NAME, message=message_bytes)
        
        return func.HttpResponse(f"Sending data was successful!", status_code=200)
