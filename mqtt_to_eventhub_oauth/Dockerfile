FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_to_eventhub.py /app/

CMD ["python", "-u", "/app/mqtt_to_eventhub.py"]