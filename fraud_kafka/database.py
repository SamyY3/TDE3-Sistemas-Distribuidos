   

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from .config import CAMINHO_BANCO, agora, formatar_timestamp
from .fraud_detector import JANELA_GEO_SEG, Transacao

ESQUEMA = """
CREATE TABLE IF NOT EXISTS transacoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  TEXT    NOT NULL UNIQUE,
    client_id       TEXT    NOT NULL,
    amount          REAL    NOT NULL CHECK (amount > 0),
    city            TEXT    NOT NULL,
    timestamp       TEXT    NOT NULL,
    is_fraud        INTEGER NOT NULL DEFAULT 0 CHECK (is_fraud IN (0, 1)),
    processed_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS alertas (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    transacao_id  INTEGER NOT NULL REFERENCES transacoes(id) ON DELETE CASCADE,
    fraud_type    TEXT    NOT NULL CHECK (fraud_type IN ('ALTO_VALOR', 'TEMPO_60s', 'GEO_10m')),
    created_at    TEXT    NOT NULL,
    UNIQUE (transacao_id, fraud_type)
);

CREATE TABLE IF NOT EXISTS mensagens_invalidas (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    conteudo     TEXT NOT NULL,
    motivo       TEXT NOT NULL,
    recebida_em  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transacoes_cliente_hora ON transacoes (client_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_alertas_transacao ON alertas (transacao_id);

CREATE VIEW IF NOT EXISTS vw_fraudes AS
SELECT t.transaction_id, t.client_id, t.amount, t.city, t.timestamp, a.fraud_type, a.created_at
FROM alertas a
JOIN transacoes t ON t.id = a.transacao_id
ORDER BY t.id, a.id;
"""


def conectar(caminho: str | Path | None = None) -> sqlite3.Connection:
                                                                               
    caminho = Path(caminho) if caminho is not None else CAMINHO_BANCO
    caminho.parent.mkdir(parents=True, exist_ok=True)
    conexao = sqlite3.connect(caminho, timeout=10)
    conexao.row_factory = sqlite3.Row
    conexao.execute("PRAGMA foreign_keys = ON")
    conexao.executescript(ESQUEMA)
    conexao.commit()
    return conexao


def transacao_existe(conexao: sqlite3.Connection, transaction_id: str) -> bool:
    linha = conexao.execute(
        "SELECT 1 FROM transacoes WHERE transaction_id = ?", (transaction_id,)
    ).fetchone()
    return linha is not None


def salvar_transacao(conexao: sqlite3.Connection, tx: Transacao, fraudes: list[str]) -> bool:
       
    momento = formatar_timestamp(agora())
    with conexao:                                                         
        cursor = conexao.execute(
            """
            INSERT INTO transacoes
                (transaction_id, client_id, amount, city, timestamp, is_fraud, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            (
                tx.transaction_id,
                tx.client_id,
                tx.amount,
                tx.city,
                formatar_timestamp(tx.timestamp),
                1 if fraudes else 0,
                momento,
            ),
        )
        if cursor.rowcount == 0:
            return False                                
        id_interno = cursor.lastrowid
        conexao.executemany(
            "INSERT INTO alertas (transacao_id, fraud_type, created_at) VALUES (?, ?, ?)",
            [(id_interno, tipo, momento) for tipo in fraudes],
        )
    return True


def salvar_mensagem_invalida(conexao: sqlite3.Connection, conteudo: str, motivo: str) -> None:
    with conexao:
        conexao.execute(
            "INSERT INTO mensagens_invalidas (conteudo, motivo, recebida_em) VALUES (?, ?, ?)",
            (conteudo, motivo, formatar_timestamp(agora())),
        )


def carregar_historico_recente(conexao: sqlite3.Connection) -> list[tuple[str, str, str]]:
                                                                                              
    linha = conexao.execute("SELECT MAX(timestamp) AS ultimo FROM transacoes").fetchone()
    if linha is None or linha["ultimo"] is None:
        return []
    corte = datetime.fromisoformat(linha["ultimo"]) - timedelta(seconds=JANELA_GEO_SEG)
    linhas = conexao.execute(
        "SELECT client_id, city, timestamp FROM transacoes WHERE timestamp >= ? ORDER BY timestamp",
        (formatar_timestamp(corte),),
    ).fetchall()
    return [(l["client_id"], l["city"], l["timestamp"]) for l in linhas]


                                                                              
                                       
                                                                              
def resumo(conexao: sqlite3.Connection) -> dict[str, int]:
    total = conexao.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0]
    fraudes = conexao.execute("SELECT COUNT(*) FROM transacoes WHERE is_fraud = 1").fetchone()[0]
    alertas = conexao.execute("SELECT COUNT(*) FROM alertas").fetchone()[0]
    invalidas = conexao.execute("SELECT COUNT(*) FROM mensagens_invalidas").fetchone()[0]
    return {
        "transacoes": total,
        "transacoes_fraudulentas": fraudes,
        "transacoes_normais": total - fraudes,
        "alertas": alertas,
        "mensagens_invalidas": invalidas,
    }


def listar_transacoes(conexao: sqlite3.Connection, limite: int = 20) -> list[sqlite3.Row]:
    return conexao.execute(
        """
        SELECT t.id, t.transaction_id, t.client_id, t.amount, t.city, t.timestamp, t.is_fraud,
               COALESCE(GROUP_CONCAT(a.fraud_type, ', '), '') AS fraudes
        FROM transacoes t
        LEFT JOIN alertas a ON a.transacao_id = t.id
        GROUP BY t.id
        ORDER BY t.id DESC
        LIMIT ?
        """,
        (limite,),
    ).fetchall()


def listar_alertas(conexao: sqlite3.Connection, limite: int = 20) -> list[sqlite3.Row]:
    return conexao.execute(
        """
        SELECT a.id, t.transaction_id, t.client_id, t.amount, t.city, t.timestamp,
               a.fraud_type, a.created_at
        FROM alertas a
        JOIN transacoes t ON t.id = a.transacao_id
        ORDER BY a.id DESC
        LIMIT ?
        """,
        (limite,),
    ).fetchall()
