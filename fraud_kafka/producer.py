   

import argparse
import os
import random
import signal
import sys
import time
from collections import deque
from datetime import timedelta

from . import cenarios, config
from .kafka_client import KafkaClient

CLIENTES = [f"CLI{n:03d}" for n in range(1, 21)]                     
CIDADES = [
    "Juazeiro do Norte",
    "Fortaleza",
    "Crato",
    "Sobral",
    "Iguatu",
    "Barbalha",
    "Recife",
    "Salvador",
]


def cidade_do_cliente(cliente: str) -> str:
                                                                                                
    indice = int(cliente[3:]) - 1
    return CIDADES[indice % len(CIDADES)]


class GeradorId:
    pass
                                                                            

    def __init__(self, caminho=config.ARQUIVO_CONTADOR) -> None:
        self.caminho = caminho
        self.ultimo = self._ler()

    def _ler(self) -> int:
        try:
            return int(self.caminho.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError):
            return 0

    def proximo(self) -> str:
        self.ultimo += 1
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        temporario = self.caminho.with_suffix(".tmp")
        temporario.write_text(str(self.ultimo), encoding="utf-8")
        os.replace(temporario, self.caminho)                                         
        return f"TXN{self.ultimo:06d}"


class GeradorAleatorio:
    def __init__(self, gerador_id: GeradorId, rng: random.Random | None = None) -> None:
        self.gerador_id = gerador_id
        self.rng = rng or random.Random()
        self.fila: deque[tuple[str, float, str]] = deque()                                  

    def _montar(self, cliente: str, valor: float, cidade: str) -> dict:
        return {
            "transaction_id": self.gerador_id.proximo(),
            "client_id": cliente,
            "amount": round(valor, 2),
            "city": cidade,
            "timestamp": config.formatar_timestamp(config.agora()),
        }

    def _valor_normal(self) -> float:
        return round(self.rng.uniform(15, 3000), 2)

    def proxima(self) -> dict:
        if self.fila:                                          
            return self._montar(*self.fila.popleft())

        cliente = self.rng.choice(CLIENTES)
        casa = cidade_do_cliente(cliente)
        sorteio = self.rng.random()

        if sorteio < 0.70:          
            return self._montar(cliente, self._valor_normal(), casa)

        if sorteio < 0.80:                                              
            valor = 10000.00 if self.rng.random() < 0.2 else self.rng.uniform(10000.01, 30000)
            return self._montar(cliente, valor, casa)

        if sorteio < 0.90:                                                              
            for _ in range(3):
                self.fila.append((cliente, self._valor_normal(), casa))
            return self._montar(cliente, self._valor_normal(), casa)

                                                                                
        outra = self.rng.choice([c for c in CIDADES if c != casa])
        self.fila.append((cliente, self._valor_normal(), outra))
        return self._montar(cliente, self._valor_normal(), casa)


def criar_produtor(cliente_kafka: KafkaClient | None = None):
    cliente = cliente_kafka if cliente_kafka is not None else KafkaClient()
    return cliente.producer(client_id="tde03-produtor")


def publicar(produtor, transacao: dict, tentativas: int = 3) -> bool:
                                                                                                   
    from kafka.errors import KafkaError

    for tentativa in range(1, tentativas + 1):
        try:
                                                                                                
            metadados = produtor.send(
                config.TOPICO, key=transacao["client_id"], value=transacao
            ).get(timeout=10)
            hora = config.formatar_timestamp(config.agora()).replace("T", " ")
            print(
                f"[{hora}] PUBLICADA {transacao['transaction_id']} | {transacao['client_id']} | "
                f"{config.formatar_reais(transacao['amount'])} | {transacao['city']} | "
                f"{transacao['timestamp']} | particao {metadados.partition} offset {metadados.offset}"
            )
            return True
        except KafkaError as erro:
            print(f"[kafka] Falha ao publicar (tentativa {tentativa}/{tentativas}): {erro}")
            time.sleep(2)
    print(f"[kafka] Transacao {transacao['transaction_id']} NAO foi publicada.")
    return False


def esperar(segundos: float, parar: dict) -> None:
                                                            
    fim = time.monotonic() + segundos
    while not parar["sinal"] and time.monotonic() < fim:
        time.sleep(0.1)


def publicar_cenarios(produtor, gerador_id: GeradorId, pausa: float, parar: dict) -> int:
                                                                                   
    enviadas = 0
    namespace_cliente = f"DEMO{gerador_id.ultimo + 1:06d}"
    for cenario in cenarios.CENARIOS:
        if parar["sinal"]:
            break
                                                                                    
        base = config.agora() - timedelta(seconds=cenarios.duracao_segundos(cenario) + 5)
        print(f"\n--- Cenario {cenario.nome}: {cenario.descricao}")
        for transacao in cenarios.gerar_transacoes(
            cenario,
            base,
            gerador_id.proximo,
            namespace_cliente=namespace_cliente,
        ):
            if parar["sinal"]:
                break
            if publicar(produtor, transacao):
                enviadas += 1
            esperar(pausa, parar)
    return enviadas


def main() -> int:
    config.configurar_console()
    argumentos = argparse.ArgumentParser(description="Produtor de transacoes (TDE 03)")
    argumentos.add_argument("--intervalo", type=float, default=3.0, help="segundos entre transacoes (padrao 3)")
    argumentos.add_argument("--max", type=int, default=0, help="parar apos N transacoes (0 = infinito)")
    argumentos.add_argument("--cenarios", action="store_true", help="publicar os cenarios controlados de teste")
    args = argumentos.parse_args()

    parar = {"sinal": False}

    def pedir_parada(signum, frame):
        parar["sinal"] = True
        print("\n[produtor] Parada solicitada, finalizando com seguranca...")

    signal.signal(signal.SIGINT, pedir_parada)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, pedir_parada)

    from kafka.errors import KafkaError

    print("=" * 66)
    print(" PRODUTOR - TRANSACOES SIMULADAS (TDE 03)")
    print(" Horarios em Brasilia (UTC-03:00)")
    print("=" * 66)

    try:
        cliente_kafka = KafkaClient()
        cliente_kafka.ensure_topic()
        produtor = criar_produtor(cliente_kafka)
    except (ConnectionError, KafkaError) as erro:
        print(f"[kafka] Nao foi possivel conectar: {erro}")
        print("[kafka] Confira se o Docker Desktop esta aberto e rode: docker compose up -d")
        return 1

    gerador_id = GeradorId()
    enviadas = 0
    try:
        if args.cenarios:
            enviadas = publicar_cenarios(produtor, gerador_id, pausa=0.5, parar=parar)
        else:
            print(f"[produtor] Publicando no topico '{config.TOPICO}' a cada {args.intervalo} s (Ctrl+C para parar)\n")
            gerador = GeradorAleatorio(gerador_id)
            while not parar["sinal"] and (args.max == 0 or enviadas < args.max):
                if publicar(produtor, gerador.proxima()):
                    enviadas += 1
                esperar(args.intervalo, parar)
    except KeyboardInterrupt:
        print("\n[produtor] Parada solicitada, finalizando com seguranca...")
    finally:
        try:
            produtor.flush(timeout=10)                                   
            produtor.close(timeout=10)
        except Exception as erro:                
            print(f"[kafka] Aviso ao fechar o produtor: {erro}")
        print(f"[produtor] Encerrado. Transacoes publicadas nesta execucao: {enviadas}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
