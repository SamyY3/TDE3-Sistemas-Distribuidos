   

import json
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass, field

from . import config, database
from .kafka_client import KafkaClient
from .fraud_detector import (
    ALTO_VALOR,
    GEO_10M,
    TEMPO_60S,
    DetectorFraudes,
    Transacao,
    TransacaoInvalida,
    validar_transacao,
)

DESCRICAO_FRAUDE = {
    ALTO_VALOR: "valor igual ou acima de R$ 10.000,00",
    TEMPO_60S: "4 transacoes do mesmo cliente em menos de 60 segundos",
    GEO_10M: "cidades diferentes para o mesmo cliente em ate 10 minutos",
}

PROCESSADA = "processada"
DUPLICADA = "duplicada"
INVALIDA = "invalida"


@dataclass
class Resultado:
    status: str
    transacao: Transacao | None = None
    fraudes: list[str] = field(default_factory=list)
    motivo: str | None = None


def processar_mensagem(conexao: sqlite3.Connection, detector: DetectorFraudes, bruto) -> Resultado:
       
    texto = ""
    try:
        if bruto is None:
            raise TransacaoInvalida("mensagem vazia")
        texto = bruto.decode("utf-8") if isinstance(bruto, (bytes, bytearray)) else str(bruto)
        try:
            dados = json.loads(texto)
        except json.JSONDecodeError as erro:
            raise TransacaoInvalida(f"JSON invalido ({erro.msg})") from None
        tx = validar_transacao(dados)
    except (TransacaoInvalida, UnicodeDecodeError) as erro:
        motivo = str(erro)
        database.salvar_mensagem_invalida(conexao, texto[:1000], motivo)
        return Resultado(INVALIDA, motivo=motivo)

    if database.transacao_existe(conexao, tx.transaction_id):
        return Resultado(DUPLICADA, transacao=tx)

    fraudes = detector.avaliar(tx)
    if not database.salvar_transacao(conexao, tx, fraudes):
        return Resultado(DUPLICADA, transacao=tx)
    detector.registrar(tx)                                         
    return Resultado(PROCESSADA, transacao=tx, fraudes=fraudes)


def exibir_resultado(resultado: Resultado, particao: int | None = None, offset: int | None = None) -> None:
    hora = config.formatar_timestamp(config.agora()).replace("T", " ")
    origem = f" Kafka: particao {particao}, offset {offset}" if offset is not None else ""
    print(f"\n[{hora}]{origem}")

    if resultado.status == INVALIDA:
        print(f"  MENSAGEM INVALIDA IGNORADA: {resultado.motivo}")
        print("  (registrada na tabela mensagens_invalidas)")
        return

    tx = resultado.transacao
    print(f"  Transacao : {tx.transaction_id}")
    print(f"  Cliente   : {tx.client_id}")
    print(f"  Valor     : {config.formatar_reais(tx.amount)}")
    print(f"  Cidade    : {tx.city}")
    print(f"  Horario   : {config.formatar_timestamp(tx.timestamp)}")

    if resultado.status == DUPLICADA:
        print("  Status    : DUPLICADA - ja existia no banco, ignorada (nenhum alerta repetido)")
        return

    if resultado.fraudes:
        print("  Status    : FRAUDE DETECTADA")
        print("  " + "!" * 60)
        for tipo in resultado.fraudes:
            print(f"  !!! ALERTA DE FRAUDE: {tipo} - {DESCRICAO_FRAUDE[tipo]}")
        print("  " + "!" * 60)
    else:
        print("  Status    : NORMAL")
    print("  (salva no banco de dados)")


def processar_com_retry(conexao, detector, bruto, tentativas: int = 3) -> Resultado | None:
                                                                                           
    for tentativa in range(1, tentativas + 1):
        try:
            return processar_mensagem(conexao, detector, bruto)
        except sqlite3.Error as erro:
            print(f"[banco] Erro ao gravar (tentativa {tentativa}/{tentativas}): {erro}")
            time.sleep(min(2 * tentativa, 5))
    return None


def main() -> int:
    config.configurar_console()

                                                                                        
    from kafka.errors import KafkaError

    parar = {"sinal": False}

    def pedir_parada(signum, frame):
        parar["sinal"] = True
        print("\n[consumidor] Parada solicitada, finalizando com seguranca...")

    signal.signal(signal.SIGINT, pedir_parada)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, pedir_parada)

    print("=" * 66)
    print(" CONSUMIDOR - DETECCAO DE FRAUDES (TDE 03)")
    print("=" * 66)

    try:
        conexao = database.conectar()
    except (sqlite3.Error, OSError) as erro:
        print(f"[banco] Nao foi possivel abrir o banco {config.CAMINHO_BANCO}: {erro}")
        return 1
    print(f"[banco] Banco pronto: {config.CAMINHO_BANCO}")

    detector = DetectorFraudes()
    try:
        carregadas = detector.carregar_historico(database.carregar_historico_recente(conexao))
        if carregadas:
            print(f"[detector] Historico recente recuperado do banco ({carregadas} transacoes).")
    except sqlite3.Error as erro:
        print(f"[banco] Aviso: nao consegui recuperar o historico: {erro}")

    try:
        cliente_kafka = KafkaClient()
        cliente_kafka.ensure_topic()
        consumidor = cliente_kafka.consumer(client_id="tde03-consumidor")
    except (ConnectionError, KafkaError) as erro:
        print(f"[kafka] Nao foi possivel conectar: {erro}")
        print("[kafka] Confira se o Docker Desktop esta aberto e rode: docker compose up -d")
        conexao.close()
        return 1

    print(f"[kafka] Conectado em {config.KAFKA_BOOTSTRAP}, topico '{config.TOPICO}'.")
    print("[consumidor] Aguardando transacoes... (Ctrl+C para parar)")

    total = 0
    try:
        while not parar["sinal"]:
            try:
                lotes = consumidor.poll(timeout_ms=1000, max_records=1)
            except KafkaError as erro:
                print(f"[kafka] Erro ao ler mensagens: {erro}. Tentando de novo em 3 s...")
                time.sleep(3)
                continue

            for particao, mensagens in lotes.items():
                for mensagem in mensagens:
                    resultado = processar_com_retry(conexao, detector, mensagem.value)
                    if resultado is None:
                                                                                         
                        print("[banco] Nao consegui gravar. Vou tentar esta mensagem de novo...")
                        consumidor.seek(particao, mensagem.offset)
                        time.sleep(3)
                        continue
                    exibir_resultado(resultado, mensagem.partition, mensagem.offset)
                    try:
                        consumidor.commit()
                        total += 1
                    except KafkaError as erro:
                                                                                          
                        print(f"[kafka] Aviso: nao consegui confirmar o offset: {erro}")
    except KeyboardInterrupt:
        print("\n[consumidor] Parada solicitada, finalizando com seguranca...")
    finally:
        try:
            consumidor.close()
        except Exception as erro:                
            print(f"[kafka] Aviso ao fechar o consumidor: {erro}")
        conexao.close()
        print(f"[consumidor] Encerrado. Mensagens processadas nesta execucao: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
