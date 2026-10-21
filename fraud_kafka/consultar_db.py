   

import argparse
import sys

from . import config, database
from .config import formatar_reais


def main() -> int:
    config.configurar_console()
    argumentos = argparse.ArgumentParser(description="Consulta o banco de fraudes")
    argumentos.add_argument("--limite", type=int, default=20, help="quantidade de linhas por tabela")
    args = argumentos.parse_args()

    if not config.CAMINHO_BANCO.exists():
        print(f"Banco ainda nao existe: {config.CAMINHO_BANCO}")
        print("Rode o consumidor (python -m fraud_kafka.consumer) pelo menos uma vez.")
        return 1

    conexao = database.conectar()
    try:
        dados = database.resumo(conexao)
        print(f"Banco: {config.CAMINHO_BANCO}\n")
        print("RESUMO")
        print(f"  Transacoes gravadas        : {dados['transacoes']}")
        print(f"  Transacoes normais         : {dados['transacoes_normais']}")
        print(f"  Transacoes com fraude      : {dados['transacoes_fraudulentas']}")
        print(f"  Alertas (tipos de fraude)  : {dados['alertas']}")
        print(f"  Mensagens invalidas        : {dados['mensagens_invalidas']}")

        print(f"\nULTIMAS {args.limite} TRANSACOES (mais recentes primeiro)")
        print(f"  {'ID':<10} {'CLIENTE':<8} {'VALOR':>14}  {'CIDADE':<18} {'HORARIO':<19} FRAUDES")
        for l in database.listar_transacoes(conexao, args.limite):
            print(
                f"  {l['transaction_id']:<10} {l['client_id']:<8} {formatar_reais(l['amount']):>14}  "
                f"{l['city']:<18} {l['timestamp']:<19} {l['fraudes'] or '-'}"
            )

        print(f"\nULTIMOS {args.limite} ALERTAS")
        print(f"  {'ID':<10} {'CLIENTE':<8} {'VALOR':>14}  {'CIDADE':<18} {'HORARIO':<19} TIPO")
        for l in database.listar_alertas(conexao, args.limite):
            print(
                f"  {l['transaction_id']:<10} {l['client_id']:<8} {formatar_reais(l['amount']):>14}  "
                f"{l['city']:<18} {l['timestamp']:<19} {l['fraud_type']}"
            )
    finally:
        conexao.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
