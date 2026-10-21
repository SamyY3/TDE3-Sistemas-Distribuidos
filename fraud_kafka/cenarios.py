   

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from .config import formatar_timestamp
from .fraud_detector import ALTO_VALOR, GEO_10M, TEMPO_60S

FORTALEZA = "Fortaleza"
SOBRAL = "Sobral"
CRATO = "Crato"
JUAZEIRO = "Juazeiro do Norte"


@dataclass(frozen=True)
class Cenario:
    nome: str
    descricao: str
                                                                 
    eventos: tuple[tuple[str, float, str, int], ...]
                                                        
    esperado: tuple[frozenset[str], ...]


def _nada() -> frozenset[str]:
    return frozenset()


def _f(*tipos: str) -> frozenset[str]:
    return frozenset(tipos)


CENARIOS: tuple[Cenario, ...] = (
    Cenario(
        "01_normal",
        "Transacao normal (R$ 150,00)",
        (("CLI901", 150.00, JUAZEIRO, 0),),
        (_nada(),),
    ),
    Cenario(
        "02_valor_exato_10000",
        "Valor exatamente R$ 10.000,00 -> ALTO_VALOR (limite inclusivo)",
        (("CLI902", 10000.00, FORTALEZA, 0),),
        (_f(ALTO_VALOR),),
    ),
    Cenario(
        "03_valor_acima_10000",
        "Valor R$ 12.500,00 -> ALTO_VALOR",
        (("CLI903", 12500.00, FORTALEZA, 0),),
        (_f(ALTO_VALOR),),
    ),
    Cenario(
        "03b_valor_abaixo_limite",
        "Valor R$ 9.999,99 -> NAO e fraude",
        (("CLI913", 9999.99, FORTALEZA, 0),),
        (_nada(),),
    ),
    Cenario(
        "04_quatro_em_menos_de_60s",
        "4 transacoes do mesmo cliente em 45 s -> TEMPO_60s na 4a",
        (
            ("CLI904", 50.00, CRATO, 0),
            ("CLI904", 60.00, CRATO, 15),
            ("CLI904", 70.00, CRATO, 30),
            ("CLI904", 80.00, CRATO, 45),
        ),
        (_nada(), _nada(), _nada(), _f(TEMPO_60S)),
    ),
    Cenario(
        "04b_quatro_em_59s",
        "4 transacoes do mesmo cliente em 59 s (limite por baixo) -> TEMPO_60s na 4a",
        (
            ("CLI914", 50.00, CRATO, 0),
            ("CLI914", 60.00, CRATO, 20),
            ("CLI914", 70.00, CRATO, 40),
            ("CLI914", 80.00, CRATO, 59),
        ),
        (_nada(), _nada(), _nada(), _f(TEMPO_60S)),
    ),
    Cenario(
        "05_quatro_em_exatos_60s",
        "4 transacoes do mesmo cliente com exatamente 60 s entre a 1a e a 4a -> NAO e fraude",
        (
            ("CLI905", 50.00, CRATO, 0),
            ("CLI905", 60.00, CRATO, 20),
            ("CLI905", 70.00, CRATO, 40),
            ("CLI905", 80.00, CRATO, 60),
        ),
        (_nada(), _nada(), _nada(), _nada()),
    ),
    Cenario(
        "06_cidades_diferentes_em_5min",
        "Fortaleza e Sobral com 5 min de diferenca -> GEO_10m na 2a",
        (
            ("CLI906", 200.00, FORTALEZA, 0),
            ("CLI906", 220.00, SOBRAL, 300),
        ),
        (_nada(), _f(GEO_10M)),
    ),
    Cenario(
        "06b_cidades_diferentes_em_10min_exatos",
        "Cidades diferentes com exatamente 10 min -> GEO_10m (limite inclusivo)",
        (
            ("CLI916", 200.00, FORTALEZA, 0),
            ("CLI916", 220.00, SOBRAL, 600),
        ),
        (_nada(), _f(GEO_10M)),
    ),
    Cenario(
        "06c_cidades_diferentes_em_10min_e_1s",
        "Cidades diferentes com 10 min e 1 s -> NAO e fraude",
        (
            ("CLI917", 200.00, FORTALEZA, 0),
            ("CLI917", 220.00, SOBRAL, 601),
        ),
        (_nada(), _nada()),
    ),
    Cenario(
        "07_cidades_diferentes_em_11min",
        "Cidades diferentes com 11 min de diferenca -> NAO e fraude",
        (
            ("CLI907", 200.00, FORTALEZA, 0),
            ("CLI907", 220.00, SOBRAL, 660),
        ),
        (_nada(), _nada()),
    ),
    Cenario(
        "07b_mesma_cidade_em_5min",
        "Mesma cidade com 5 min de diferenca -> NAO e fraude",
        (
            ("CLI918", 200.00, FORTALEZA, 0),
            ("CLI918", 220.00, FORTALEZA, 300),
        ),
        (_nada(), _nada()),
    ),
    Cenario(
        "08_clientes_diferentes",
        "4 transacoes em 30 s e cidades diferentes, mas de clientes DIFERENTES -> nada dispara",
        (
            ("CLI908", 100.00, FORTALEZA, 0),
            ("CLI909", 100.00, SOBRAL, 10),
            ("CLI910", 100.00, CRATO, 20),
            ("CLI911", 100.00, JUAZEIRO, 30),
        ),
        (_nada(), _nada(), _nada(), _nada()),
    ),
    Cenario(
        "09_multiplas_fraudes",
        "Uma transacao dispara ALTO_VALOR + TEMPO_60s + GEO_10m ao mesmo tempo",
        (
            ("CLI912", 100.00, FORTALEZA, 0),
            ("CLI912", 100.00, FORTALEZA, 10),
            ("CLI912", 100.00, FORTALEZA, 20),
            ("CLI912", 15000.00, SOBRAL, 30),
        ),
        (_nada(), _nada(), _nada(), _f(ALTO_VALOR, TEMPO_60S, GEO_10M)),
    ),
)


def duracao_segundos(cenario: Cenario) -> int:
    return max(evento[3] for evento in cenario.eventos)


def gerar_transacoes(
    cenario: Cenario,
    base: datetime,
    proximo_id: Callable[[], str],
    namespace_cliente: str | None = None,
) -> list[dict]:
                                                                                           
    transacoes = []
    for cliente, valor, cidade, deslocamento in cenario.eventos:
        transacoes.append(
            {
                "transaction_id": proximo_id(),
                "client_id": (
                    f"{cliente}-{namespace_cliente}" if namespace_cliente else cliente
                ),
                "amount": valor,
                "city": cidade,
                "timestamp": formatar_timestamp(base + timedelta(seconds=deslocamento)),
            }
        )
    return transacoes
