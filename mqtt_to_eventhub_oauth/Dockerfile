ARG BUILD_FROM=ghcr.io/home-assistant/base:python3.12
FROM ${BUILD_FROM}

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_to_eventhub.py /app/
COPY run.sh /app/
RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]