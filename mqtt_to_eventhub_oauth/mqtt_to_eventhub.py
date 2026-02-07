import json
import time
import queue
import threading
import logging
import datetime
from typing import Dict, Any

import paho.mqtt.client as mqtt
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData

OPTIONS_PATH = "/data/options.json"


def load_options() -> Dict[str, Any]:
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )


class Forwarder:
    def __init__(
        self,
        producer: EventHubProducerClient,
        max_batch_size: int,
        max_batch_wait_ms: int,
        queue_maxsize: int
    ):
        self.producer = producer
        self.max_batch_size = max_batch_size
        self.max_batch_wait_ms = max_batch_wait_ms
        self.q: "queue.Queue[tuple[str, bytes, int]]" = queue.Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def put(self, topic: str, payload: bytes, qos: int) -> None:
        try:
            self.q.put_nowait((topic, payload, qos))
        except queue.Full:
            logging.warning("Queue full; dropping message from topic=%s", topic)

    def _run(self) -> None:
        flush_deadline = time.time() + (self.max_batch_wait_ms / 1000.0)
        batch = None
        count = 0

        while not self._stop.is_set():
            timeout = max(0.0, flush_deadline - time.time())
            try:
                topic, payload, qos = self.q.get(timeout=timeout)

                if batch is None:
                    batch = self.producer.create_batch()

                evt = EventData(payload)
                evt.properties = {
                    "mqtt_topic": topic,
                    "mqtt_qos": int(qos),  # pidä pienenä
                    "ingest_ts": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                }

                try:
                    batch.add(evt)
                    count += 1
                except ValueError:
                    # Batch full
                    self.producer.send_batch(batch)
                    batch = self.producer.create_batch()
                    batch.add(evt)
                    count = 1

                if count >= self.max_batch_size:
                    self.producer.send_batch(batch)
                    batch = None
                    count = 0
                    flush_deadline = time.time() + (self.max_batch_wait_ms / 1000.0)

            except queue.Empty:
                # Time flush
                if batch is not None and count > 0:
                    self.producer.send_batch(batch)
                    batch = None
                    count = 0
                flush_deadline = time.time() + (self.max_batch_wait_ms / 1000.0)

            except Exception as e:
                logging.exception("Forwarder error: %s", e)
                time.sleep(1)


def main() -> None:
    opt = load_options()
    setup_logging(opt.get("log_level", "INFO"))

    # MQTT
    mqtt_host = opt["mqtt_host"]
    mqtt_port = int(opt.get("mqtt_port", 1883))
    mqtt_username = opt.get("mqtt_username") or None
    mqtt_password = opt.get("mqtt_password") or None
    mqtt_topic = opt.get("mqtt_topic", "home/#")
    mqtt_qos = int(opt.get("mqtt_qos", 0))
    mqtt_client_id = opt.get("client_id_mqtt", "ha-mqtt-to-eventhub")

    # Event Hubs (OAuth)
    eventhub_fqdn = opt["eventhub_fqdn"]  # e.g. YOURNAMESPACE.servicebus.windows.net
    eventhub_name = opt["eventhub_name"]

    tenant_id = opt["tenant_id"]
    client_id = opt["client_id"]
    client_secret = opt["client_secret"]

    for k, v in [("eventhub_fqdn", eventhub_fqdn), ("eventhub_name", eventhub_name),
                 ("tenant_id", tenant_id), ("client_id", client_id), ("client_secret", client_secret)]:
        if not v:
            raise SystemExit(f"Missing required option: {k}")

    max_batch_size = int(opt.get("max_batch_size", 200))
    max_batch_wait_ms = int(opt.get("max_batch_wait_ms", 1000))
    queue_maxsize = int(opt.get("queue_maxsize", 20000))

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    producer = EventHubProducerClient(
        fully_qualified_namespace=eventhub_fqdn,
        eventhub_name=eventhub_name,
        credential=credential
    )

    forwarder = Forwarder(
        producer=producer,
        max_batch_size=max_batch_size,
        max_batch_wait_ms=max_batch_wait_ms,
        queue_maxsize=queue_maxsize
    )
    forwarder.start()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            logging.info("Connected to MQTT broker %s:%s", mqtt_host, mqtt_port)
            client.subscribe(mqtt_topic, qos=mqtt_qos)
            logging.info("Subscribed to topic: %s (qos=%s)", mqtt_topic, mqtt_qos)
        else:
            logging.error("MQTT connect failed with rc=%s", rc)

    def on_message(client, userdata, msg):
        forwarder.put(msg.topic, msg.payload, msg.qos)

    def on_disconnect(client, userdata, rc, properties=None):
        logging.warning("Disconnected from MQTT (rc=%s). Reconnecting...", rc)

    mqttc = mqtt.Client(client_id=mqtt_client_id, protocol=mqtt.MQTTv311)

    if mqtt_username is not None and mqtt_password is not None:
        mqttc.username_pw_set(mqtt_username, mqtt_password)

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect
    mqttc.reconnect_delay_set(min_delay=1, max_delay=30)

    logging.info("Starting MQTT loop...")
    mqttc.connect(mqtt_host, mqtt_port, keepalive=60)
    mqttc.loop_forever()


if __name__ == "__main__":
    main()