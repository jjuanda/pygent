import logging

from .AsyncConsumer import AsyncConsumer
from .Publisher import Publisher

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Processor(AsyncConsumer):
    """docstring for Processor."""

    def __init__(self, cfg):
        super().__init__(cfg)
        self.publish_queues = {}


    def on_message(self, channel, basic_deliver, properties, body):
        print(channel)
        print(basic_deliver)
        print(properties)
        print(body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def add_publisher(self, name, pcfg, connect=True):
        cfg = {
            "amqp_url"      : self.cfg["amqp_url"],
            "exchange"      : pcfg["exchange"],
            "exchange_type" : pcfg.get("exchange_type", "topic"),
            "durable"       : pcfg.get("durable", True),
            "queue"         : pcfg["queue"],
            "routing_key"   : pcfg["routing_key"],
            "app_id"        : pcfg["app_id"]
        }

        q = Publisher(cfg)
        if connect:
            q.connect()

        self.publish_queues[name] = q

    def stop():
        self.stop()
        for p in self.publish_queues:
            p.stop()

    def send(self, qname, msg):
        self.publish_queues[qname].send(msg)

    def send_json(self, qname, d):
        self.publish_queues[qname].send_json(d)

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    cfg = {
        "amqp_url"      : "amqp://guest:guest@localhost:5672/%2F",
        "exchange"      : "message",
        "exchange_type" : "fanout",
        "queue"         : "text",
        "durable"       : True,
        "routing_key"   : "example.text",
        "app_id"        : "processor"
    }
    p = Processor(cfg)
    try:
        p.run()
    except KeyboardInterrupt:
        p.stop()
