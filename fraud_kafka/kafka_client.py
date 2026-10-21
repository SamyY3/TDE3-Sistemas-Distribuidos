                                                                       

from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPICO", "transacoes")
KAFKA_GROUP = os.environ.get("KAFKA_GRUPO", "detector-fraudes")


class KafkaClient:
    pass
       

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
    ) -> None:
        self.bootstrap_servers = (
            KAFKA_BOOTSTRAP if bootstrap_servers is None else bootstrap_servers
        )
        self.topic = KAFKA_TOPIC if topic is None else topic
        self.group_id = KAFKA_GROUP if group_id is None else group_id

    def ensure_topic(self, tentativas: int = 12, intervalo: float = 5.0) -> None:
                                                                       
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import KafkaError, TopicAlreadyExistsError

        ultimo_erro = None
        for tentativa in range(1, tentativas + 1):
            admin = None
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id="tde03-admin",
                )
                if self.topic in admin.list_topics():
                    return
                try:
                    admin.create_topics(
                        [NewTopic(name=self.topic, num_partitions=1, replication_factor=1)]
                    )
                    print(f"[kafka] Topico '{self.topic}' criado.")
                except TopicAlreadyExistsError:
                    pass
                return
            except KafkaError as erro:
                ultimo_erro = erro
                print(
                    f"[kafka] Nao consegui conectar em {self.bootstrap_servers} "
                    f"(tentativa {tentativa}/{tentativas}). "
                    "O Docker/Kafka esta rodando? Tentando de novo..."
                )
                time.sleep(intervalo)
            finally:
                if admin is not None:
                    try:
                        admin.close()
                    except Exception:
                        pass
        raise ConnectionError(
            f"Kafka indisponivel em {self.bootstrap_servers} "
            f"apos {tentativas} tentativas: {ultimo_erro}"
        )

    def producer(self, **options: Any) -> KafkaProducer:
                                                                   
        from kafka import KafkaProducer

        producer_options: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": "kafka-producer",
            "acks": "all",
            "retries": 5,
            "key_serializer": lambda key: key.encode("utf-8"),
            "value_serializer": lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
        }
        producer_options.update(options)
        return KafkaProducer(**producer_options)

    def consumer(self, **options: Any) -> KafkaConsumer:
                                                                           
        from kafka import KafkaConsumer

        consumer_options: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.group_id,
            "client_id": "kafka-consumer",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "max_poll_records": 1,
        }
        consumer_options.update(options)
        return KafkaConsumer(self.topic, **consumer_options)
