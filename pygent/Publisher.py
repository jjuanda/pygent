import logging
import ujson as json

import pika

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Publisher(object):
    """docstring for Publishercfg."""

    def __init__(self, cfg):
        super().__init__()
        self.cfg = cfg
        self.connected = False

    def connect(self):
        self.params    = pika.URLParameters(self.cfg["amqp_url"])
        print(self.params)
        self.conn      = pika.BlockingConnection(self.params)
        self.channel   = self.conn.channel()
        self.exchange  = self.channel.exchange_declare(exchange=self.cfg["exchange"],
                                                       type="fanout")
        self.queue     = self.channel.queue_declare(queue=self.cfg["queue"],
                                   durable=self.cfg.get("durable", True))
        self.channel.queue_bind(exchange=self.cfg["exchange"], 
                                queue=self.queue.method.queue)
        self.connected = True

    def send(self, msg, hdrs={}, ctype="text/plain", delivery_mode=1):
        if not self.connected:
            raise ValueError("Not connected!")

        properties = pika.BasicProperties(app_id=self.cfg["app_id"],
                                          content_type=ctype,
                                          headers=hdrs)

        self.channel.basic_publish(self.cfg["exchange"],
                                   self.cfg["routing_key"],
                                   msg,
                                   properties)


    def send_json(self, d, hdrs={}, delivery_mode=1):
        self.send(json.dumps(d),
                  ctype="application/json",
                  delivery_mode=delivery_mode)

    def stop(self):
        self.connected = False
        self.conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    cfg = {
        "amqp_url"         : "amqp://guest:guest@127.0.0.1:5672/%2F?connection_attempts=3&heartbeat_interval=3600",
        "exchange"         : "message",
        "exchange_type"    : "topic",
        "queue"            : "text",
        "durable"          : True,
        "routing_key"      : "example.text",
        "app_id"           : "app_id"
    }

    p = Publisher(cfg)
    p.connect()
    p.send("hola")
