   

import bisect
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

                                                                    
ALTO_VALOR = "ALTO_VALOR"
TEMPO_60S = "TEMPO_60s"
GEO_10M = "GEO_10m"
TIPOS_FRAUDE = (ALTO_VALOR, TEMPO_60S, GEO_10M)

                       
LIMITE_ALTO_VALOR = 10000.0
QTD_TRANSACOES_TEMPO = 4
JANELA_TEMPO_SEG = 60
JANELA_GEO_SEG = 10 * 60

CAMPOS_OBRIGATORIOS = ("transaction_id", "client_id", "amount", "city", "timestamp")


class TransacaoInvalida(ValueError):
    pass
@dataclass(frozen=True)
class Transacao:
    transaction_id: str
    client_id: str
    amount: float
    city: str
    timestamp: datetime

    def para_dict(self) -> dict[str, Any]:
                                                              
        return {
            "transaction_id": self.transaction_id,
            "client_id": self.client_id,
            "amount": self.amount,
            "city": self.city,
            "timestamp": self.timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        }


def validar_transacao(dados: Any) -> Transacao:
                                                                                               
    if not isinstance(dados, dict):
        raise TransacaoInvalida("a mensagem precisa ser um objeto JSON")

    faltando = [campo for campo in CAMPOS_OBRIGATORIOS if campo not in dados]
    if faltando:
        raise TransacaoInvalida(f"campos obrigatorios ausentes: {', '.join(faltando)}")

    for campo in ("transaction_id", "client_id", "city", "timestamp"):
        valor = dados[campo]
        if not isinstance(valor, str) or not valor.strip():
            raise TransacaoInvalida(f"'{campo}' deve ser um texto nao vazio")

    amount = dados["amount"]
                                                                          
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise TransacaoInvalida("'amount' deve ser um numero")
    if not math.isfinite(amount) or amount <= 0:
        raise TransacaoInvalida("'amount' deve ser um numero positivo")

    try:
        momento = datetime.fromisoformat(dados["timestamp"].strip())
    except ValueError:
        raise TransacaoInvalida(
            "'timestamp' deve estar no formato AAAA-MM-DDTHH:MM:SS"
        ) from None
    if momento.tzinfo is not None:
        raise TransacaoInvalida(
            "'timestamp' nao deve ter fuso (use horario de Brasilia, sem sufixo)"
        )

    return Transacao(
        transaction_id=dados["transaction_id"].strip(),
        client_id=dados["client_id"].strip(),
        amount=float(amount),
        city=dados["city"].strip(),
        timestamp=momento.replace(microsecond=0),
    )


@dataclass(order=True)
class _Evento:
    timestamp: datetime
    city: str = field(compare=False)


def _normalizar_cidade(cidade: str) -> str:
    return cidade.strip().casefold()


class DetectorFraudes:
    def __init__(self) -> None:
        self._historico: dict[str, list[_Evento]] = defaultdict(list)

                                                                             
    def avaliar(self, tx: Transacao) -> list[str]:
                                                                                 
        fraudes: list[str] = []

                              
        if tx.amount >= LIMITE_ALTO_VALOR:
            fraudes.append(ALTO_VALOR)

        historico = self._historico.get(tx.client_id, [])

                                                                         
        horarios = [e.timestamp for e in historico if e.timestamp <= tx.timestamp]
        horarios.append(tx.timestamp)
        horarios.sort()
        if len(horarios) >= QTD_TRANSACOES_TEMPO:
            intervalo = (horarios[-1] - horarios[-QTD_TRANSACOES_TEMPO]).total_seconds()
            if intervalo < JANELA_TEMPO_SEG:
                fraudes.append(TEMPO_60S)

                                                        
        cidade_atual = _normalizar_cidade(tx.city)
        for evento in historico:
            diferenca = abs((tx.timestamp - evento.timestamp).total_seconds())
            if diferenca <= JANELA_GEO_SEG and _normalizar_cidade(evento.city) != cidade_atual:
                fraudes.append(GEO_10M)
                break

        return fraudes

                                                                              
    def registrar(self, tx: Transacao) -> None:
                                                                                        
        lista = self._historico[tx.client_id]
        bisect.insort(lista, _Evento(tx.timestamp, tx.city))
                                                                                   
                                                                                 
        corte = lista[-1].timestamp.timestamp() - JANELA_GEO_SEG
        while lista and lista[0].timestamp.timestamp() < corte:
            lista.pop(0)

    def processar(self, tx: Transacao) -> list[str]:
                                                          
        fraudes = self.avaliar(tx)
        self.registrar(tx)
        return fraudes

    def carregar_historico(self, linhas: list[tuple[str, str, str]]) -> int:
                                                                                              
        for client_id, cidade, timestamp in linhas:
            bisect.insort(
                self._historico[client_id], _Evento(datetime.fromisoformat(timestamp), cidade)
            )
        return len(linhas)
