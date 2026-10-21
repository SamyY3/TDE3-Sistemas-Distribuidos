   

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

                                                                              
       
                                                                              
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICO = os.environ.get("KAFKA_TOPICO", "transacoes")
GRUPO_CONSUMIDOR = os.environ.get("KAFKA_GRUPO", "detector-fraudes")

                                                                              
                                                                   
                                                                              
PASTA_PROJETO = Path(__file__).resolve().parent.parent
PASTA_DATA = PASTA_PROJETO / "data"
CAMINHO_BANCO = Path(os.environ.get("FRAUDES_DB", PASTA_DATA / "transacoes.db"))
ARQUIVO_CONTADOR = PASTA_DATA / "contador_producer.txt"

                                                                              
             
                                                                              
FUSO_BRASILIA = timezone(timedelta(hours=-3))
FORMATO_TIMESTAMP = "%Y-%m-%dT%H:%M:%S"


def agora() -> datetime:
                                                                                               
    return datetime.now(FUSO_BRASILIA).replace(tzinfo=None, microsecond=0)


def formatar_timestamp(momento: datetime) -> str:
    return momento.strftime(FORMATO_TIMESTAMP)


def formatar_reais(valor: float) -> str:
                                              
    texto = f"{valor:,.2f}"             
    texto = texto.replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {texto}"


                                                                              
         
                                                                              
def configurar_console() -> None:
                                                                                    
    for fluxo in (sys.stdout, sys.stderr):
        try:
            fluxo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


                                                                              
                                                        
                                                                              
def garantir_topico(
    bootstrap: str = KAFKA_BOOTSTRAP,
    topico: str = TOPICO,
    tentativas: int = 12,
    intervalo: float = 5.0,
) -> None:
       
    from .kafka_client import KafkaClient

    KafkaClient(bootstrap_servers=bootstrap, topic=topico).ensure_topic(
        tentativas=tentativas,
        intervalo=intervalo,
    )
