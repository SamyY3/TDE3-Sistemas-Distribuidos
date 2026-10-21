                                                                

import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from fraud_kafka.kafka_client import KafkaClient


class TestKafkaClient(unittest.TestCase):
    def setUp(self):
        self.client = KafkaClient(
            bootstrap_servers="broker:9092",
            topic="eventos",
            group_id="consumidores-teste",
        )
        self.kafka_module = ModuleType("kafka")

    def test_cria_produtor_json_configuravel(self):
        chamadas = []

        def criar_produtor(**opcoes):
            chamadas.append(opcoes)
            return "produtor"

        self.kafka_module.KafkaProducer = criar_produtor
        with patch.dict(sys.modules, {"kafka": self.kafka_module}):
            produtor = self.client.producer(client_id="meu-produtor")

        self.assertEqual(produtor, "produtor")
        opcoes = chamadas[0]
        self.assertEqual(opcoes["bootstrap_servers"], "broker:9092")
        self.assertEqual(opcoes["client_id"], "meu-produtor")
        self.assertEqual(opcoes["acks"], "all")
        self.assertEqual(opcoes["key_serializer"]("cliente"), b"cliente")
        self.assertEqual(
            opcoes["value_serializer"]({"mensagem": "Olá"}),
            '{"mensagem": "Olá"}'.encode("utf-8"),
        )

    def test_cria_consumidor_com_commit_manual(self):
        chamadas = []

        def criar_consumidor(topico, **opcoes):
            chamadas.append((topico, opcoes))
            return "consumidor"

        self.kafka_module.KafkaConsumer = criar_consumidor
        with patch.dict(sys.modules, {"kafka": self.kafka_module}):
            consumidor = self.client.consumer(max_poll_records=5)

        self.assertEqual(consumidor, "consumidor")
        topico, opcoes = chamadas[0]
        self.assertEqual(topico, "eventos")
        self.assertEqual(opcoes["bootstrap_servers"], "broker:9092")
        self.assertEqual(opcoes["group_id"], "consumidores-teste")
        self.assertFalse(opcoes["enable_auto_commit"])
        self.assertEqual(opcoes["max_poll_records"], 5)

    def test_garante_topico_no_broker_configurado(self):
        admin_module = ModuleType("kafka.admin")
        errors_module = ModuleType("kafka.errors")
        kafka_error = type("KafkaError", (Exception,), {})
        errors_module.KafkaError = kafka_error
        errors_module.TopicAlreadyExistsError = type(
            "TopicAlreadyExistsError", (kafka_error,), {}
        )
        admin = Mock()
        admin.list_topics.return_value = {"eventos"}
        administracao = Mock(return_value=admin)
        admin_module.KafkaAdminClient = administracao
        admin_module.NewTopic = Mock()
        self.kafka_module.__path__ = []

        with patch.dict(
            sys.modules,
            {
                "kafka": self.kafka_module,
                "kafka.admin": admin_module,
                "kafka.errors": errors_module,
            },
        ):
            self.client.ensure_topic(tentativas=2, intervalo=0.1)

        administracao.assert_called_once_with(
            bootstrap_servers="broker:9092",
            client_id="tde03-admin",
        )
        admin.create_topics.assert_not_called()
        admin.close.assert_called_once()

    def test_cria_topico_quando_ainda_nao_existe(self):
        admin_module = ModuleType("kafka.admin")
        errors_module = ModuleType("kafka.errors")
        kafka_error = type("KafkaError", (Exception,), {})
        errors_module.KafkaError = kafka_error
        errors_module.TopicAlreadyExistsError = type(
            "TopicAlreadyExistsError", (kafka_error,), {}
        )
        admin = Mock()
        admin.list_topics.return_value = set()
        admin_module.KafkaAdminClient = Mock(return_value=admin)
        admin_module.NewTopic = Mock(
            side_effect=lambda **opcoes: opcoes
        )
        self.kafka_module.__path__ = []

        with patch.dict(
            sys.modules,
            {
                "kafka": self.kafka_module,
                "kafka.admin": admin_module,
                "kafka.errors": errors_module,
            },
        ):
            self.client.ensure_topic()

        admin.create_topics.assert_called_once_with(
            [
                {
                    "name": "eventos",
                    "num_partitions": 1,
                    "replication_factor": 1,
                }
            ]
        )
        admin.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
