   

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

from fraud_kafka import cenarios, consumer, database
from fraud_kafka.cenarios import CENARIOS
from fraud_kafka.config import formatar_reais
from fraud_kafka.fraud_detector import (
    ALTO_VALOR,
    GEO_10M,
    TEMPO_60S,
    DetectorFraudes,
    Transacao,
    TransacaoInvalida,
    validar_transacao,
)

BASE = datetime(2026, 10, 20, 14, 30, 0)


def tx(n: int, cliente: str, valor: float, cidade: str, segundos: int) -> Transacao:
    return Transacao(f"TXN{n:06d}", cliente, valor, cidade, BASE + timedelta(seconds=segundos))


def como_dict(t: Transacao) -> dict:
    return t.para_dict()


def transacoes_do_cenario(cenario) -> list[dict]:
    contador = iter(range(1, 1000))
    return cenarios.gerar_transacoes(cenario, BASE, lambda: f"TXN{next(contador):06d}")


                                                                              
                                                                     
                                                                              
class TestRegrasDoProfessor(unittest.TestCase):
    def setUp(self):
        self.detector = DetectorFraudes()

    def test_1_transacao_normal(self):
                                                 
        self.assertEqual(self.detector.processar(tx(1, "CLI001", 150.00, "Juazeiro do Norte", 0)), [])

    def test_2_valor_exatamente_10000(self):
                                                              
        self.assertEqual(self.detector.processar(tx(1, "CLI001", 10000.00, "Fortaleza", 0)), [ALTO_VALOR])

    def test_3_valor_acima_de_10000(self):
                                                   
        self.assertEqual(self.detector.processar(tx(1, "CLI001", 12500.00, "Fortaleza", 0)), [ALTO_VALOR])

    def test_3b_valor_abaixo_do_limite(self):
                                                   
        self.assertEqual(self.detector.processar(tx(1, "CLI001", 9999.99, "Fortaleza", 0)), [])

    def test_4_quatro_transacoes_em_menos_de_60s(self):
                                                                               
        resultados = [
            self.detector.processar(tx(i + 1, "CLI001", 50.0, "Crato", i * 15)) for i in range(4)
        ]
        self.assertEqual(resultados, [[], [], [], [TEMPO_60S]])

    def test_4b_quatro_transacoes_em_59s(self):
                                                                         
        segundos = [0, 20, 40, 59]
        resultados = [
            self.detector.processar(tx(i + 1, "CLI001", 50.0, "Crato", s)) for i, s in enumerate(segundos)
        ]
        self.assertEqual(resultados[-1], [TEMPO_60S])

    def test_5_quatro_transacoes_em_exatos_60s(self):
                                                                                      
        segundos = [0, 20, 40, 60]
        resultados = [
            self.detector.processar(tx(i + 1, "CLI001", 50.0, "Crato", s)) for i, s in enumerate(segundos)
        ]
        self.assertEqual(resultados, [[], [], [], []])

    def test_5b_tres_transacoes_nao_bastam(self):
                                                          
        resultados = [
            self.detector.processar(tx(i + 1, "CLI001", 50.0, "Crato", i * 5)) for i in range(3)
        ]
        self.assertEqual(resultados, [[], [], []])

    def test_5c_janela_movel_quinta_transacao(self):
                                                                                 
        segundos = [0, 50, 100, 110, 115]                                                     
        resultados = [
            self.detector.processar(tx(i + 1, "CLI001", 50.0, "Crato", s)) for i, s in enumerate(segundos)
        ]
        self.assertEqual(resultados[-2:], [[], []])
                                                                        
        self.assertEqual(self.detector.processar(tx(6, "CLI001", 50.0, "Crato", 120)), [TEMPO_60S])

    def test_6_cidades_diferentes_dentro_de_10min(self):
                                                                         
        self.assertEqual(self.detector.processar(tx(1, "CLI001", 200, "Fortaleza", 0)), [])
        self.assertEqual(self.detector.processar(tx(2, "CLI001", 200, "Sobral", 300)), [GEO_10M])

    def test_6b_limite_de_10min_inclusivo(self):
                                                                                
        self.detector.processar(tx(1, "CLI001", 200, "Fortaleza", 0))
        self.assertEqual(self.detector.processar(tx(2, "CLI001", 200, "Sobral", 600)), [GEO_10M])

    def test_7_cidades_diferentes_fora_de_10min(self):
                                                                         
        self.detector.processar(tx(1, "CLI001", 200, "Fortaleza", 0))
        self.assertEqual(self.detector.processar(tx(2, "CLI001", 200, "Sobral", 660)), [])

    def test_7b_limite_10min_e_1s(self):
                                                                   
        self.detector.processar(tx(1, "CLI001", 200, "Fortaleza", 0))
        self.assertEqual(self.detector.processar(tx(2, "CLI001", 200, "Sobral", 601)), [])

    def test_7c_mesma_cidade_nao_e_fraude(self):
                                                                                 
        self.detector.processar(tx(1, "CLI001", 200, "Fortaleza", 0))
        self.assertEqual(self.detector.processar(tx(2, "CLI001", 200, "  fortaleza ", 60)), [])

    def test_8_clientes_diferentes_nao_se_misturam(self):
                                                                                           
        resultados = [
            self.detector.processar(tx(1, "CLI001", 100, "Fortaleza", 0)),
            self.detector.processar(tx(2, "CLI002", 100, "Sobral", 10)),
            self.detector.processar(tx(3, "CLI003", 100, "Crato", 20)),
            self.detector.processar(tx(4, "CLI004", 100, "Juazeiro do Norte", 30)),
        ]
        self.assertEqual(resultados, [[], [], [], []])

    def test_8b_historico_de_um_cliente_nao_vaza_para_outro(self):
                                                                  
        for i in range(3):
            self.detector.processar(tx(i + 1, "CLI001", 50, "Crato", i * 5))
        self.assertEqual(self.detector.processar(tx(4, "CLI002", 50, "Crato", 15)), [])

    def test_9_multiplas_fraudes_na_mesma_transacao(self):
                                                                       
        for i in range(3):
            self.detector.processar(tx(i + 1, "CLI001", 100, "Fortaleza", i * 10))
        fraudes = self.detector.processar(tx(4, "CLI001", 15000, "Sobral", 30))
        self.assertEqual(fraudes, [ALTO_VALOR, TEMPO_60S, GEO_10M])


                                                                              
                                                                             
                                                                              
class TestCenarios(unittest.TestCase):
    def test_todos_os_cenarios_produzem_o_resultado_esperado(self):
                                                                           
        for cenario in CENARIOS:
            with self.subTest(cenario=cenario.nome):
                detector = DetectorFraudes()
                transacoes = transacoes_do_cenario(cenario)
                self.assertEqual(len(transacoes), len(cenario.esperado))
                for dados, esperado in zip(transacoes, cenario.esperado):
                    obtido = frozenset(detector.processar(validar_transacao(dados)))
                    self.assertEqual(obtido, esperado, f"{cenario.nome}: {dados}")

    def test_cenarios_usam_clientes_proprios(self):
                                                                                           
        clientes_por_cenario = {c.nome: {e[0] for e in c.eventos} for c in CENARIOS}
        todos = [cliente for clientes in clientes_por_cenario.values() for cliente in clientes]
        usados_em_mais_de_um = {c for c in todos if todos.count(c) > 1}
        self.assertEqual(usados_em_mais_de_um, set())

    def test_namespace_isola_clientes_entre_execucoes(self):
                                                                             
        primeira = cenarios.gerar_transacoes(
            CENARIOS[0],
            BASE,
            lambda: "TXN000001",
            namespace_cliente="DEMO000001",
        )
        segunda = cenarios.gerar_transacoes(
            CENARIOS[0],
            BASE,
            lambda: "TXN000002",
            namespace_cliente="DEMO000002",
        )

        self.assertEqual(primeira[0]["amount"], segunda[0]["amount"])
        self.assertEqual(primeira[0]["timestamp"], segunda[0]["timestamp"])
        self.assertNotEqual(primeira[0]["client_id"], segunda[0]["client_id"])
        self.assertNotEqual(primeira[0]["transaction_id"], segunda[0]["transaction_id"])


                                                                              
                         
                                                                              
class TestValidacao(unittest.TestCase):
    valida = {
        "transaction_id": "TXN000001",
        "client_id": "CLI001",
        "amount": 150.0,
        "city": "Juazeiro do Norte",
        "timestamp": "2026-10-20T14:30:00",
    }

    def test_mensagem_valida(self):
                                                 
        t = validar_transacao(dict(self.valida))
        self.assertEqual(t.para_dict(), self.valida)

    def test_amount_inteiro_e_aceito(self):
                                                
        self.assertEqual(validar_transacao({**self.valida, "amount": 150}).amount, 150.0)

    def test_campos_invalidos_sao_rejeitados(self):
                                                                              
        casos = [
            "texto solto",
            {k: v for k, v in self.valida.items() if k != "city"},
            {**self.valida, "amount": "150"},
            {**self.valida, "amount": True},
            {**self.valida, "amount": -5},
            {**self.valida, "amount": 0},
            {**self.valida, "amount": float("nan")},
            {**self.valida, "client_id": ""},
            {**self.valida, "client_id": 7},
            {**self.valida, "timestamp": "20/10/2026 14:30"},
            {**self.valida, "timestamp": "2026-10-20T14:30:00-03:00"},
        ]
        for caso in casos:
            with self.subTest(caso=caso):
                with self.assertRaises(TransacaoInvalida):
                    validar_transacao(caso)


                                                                              
                                                                               
                                                                              
class TestBancoEConsumidor(unittest.TestCase):
    def setUp(self):
        self.pasta = tempfile.TemporaryDirectory()
        self.caminho = Path(self.pasta.name) / "teste.db"
        self.conexao = database.conectar(self.caminho)
        self.detector = DetectorFraudes()

    def tearDown(self):
        self.conexao.close()
        self.pasta.cleanup()

    def enviar(self, dados) -> consumer.Resultado:
        bruto = json.dumps(dados).encode("utf-8")                              
        return consumer.processar_mensagem(self.conexao, self.detector, bruto)

    def test_tabelas_criadas_automaticamente(self):
                                                              
        tabelas = {l[0] for l in self.conexao.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"transacoes", "alertas", "mensagens_invalidas"} <= tabelas)

    def test_salva_transacoes_normais_e_fraudulentas(self):
                                                                             
        self.enviar(tx(1, "CLI001", 150, "Fortaleza", 0).para_dict())
        self.enviar(tx(2, "CLI002", 12500, "Fortaleza", 0).para_dict())
        resumo = database.resumo(self.conexao)
        self.assertEqual(resumo["transacoes"], 2)
        self.assertEqual(resumo["transacoes_normais"], 1)
        self.assertEqual(resumo["transacoes_fraudulentas"], 1)

    def test_alertas_vinculados_a_transacao_e_multiplas_fraudes(self):
                                                                                   
        for i in range(3):
            self.enviar(tx(i + 1, "CLI001", 100, "Fortaleza", i * 10).para_dict())
        resultado = self.enviar(tx(4, "CLI001", 15000, "Sobral", 30).para_dict())
        self.assertEqual(resultado.fraudes, [ALTO_VALOR, TEMPO_60S, GEO_10M])
        linhas = self.conexao.execute(
            "SELECT t.transaction_id, a.fraud_type FROM alertas a JOIN transacoes t ON t.id = a.transacao_id"
        ).fetchall()
        self.assertEqual({(l[0], l[1]) for l in linhas}, {("TXN000004", ALTO_VALOR), ("TXN000004", TEMPO_60S), ("TXN000004", GEO_10M)})
        marcada = self.conexao.execute("SELECT is_fraud FROM transacoes WHERE transaction_id='TXN000004'").fetchone()[0]
        self.assertEqual(marcada, 1)

    def test_alertas_das_tres_regras_aparecem_no_console_e_no_banco(self):
                                                                                   
        sequencias = (
            [
                tx(101, "CLI101", 9999.99, "Fortaleza", 0),
                tx(102, "CLI102", 10000.00, "Fortaleza", 0),
                tx(103, "CLI103", 12500.00, "Fortaleza", 0),
            ],
            [
                tx(201, "CLI201", 50, "Crato", 0),
                tx(202, "CLI201", 50, "Crato", 20),
                tx(203, "CLI201", 50, "Crato", 40),
                tx(204, "CLI201", 50, "Crato", 59),
            ],
            [
                tx(301, "CLI301", 200, "Fortaleza", 0),
                tx(302, "CLI301", 200, "Sobral", 300),
            ],
        )
        alertas_esperados = {
            "TXN000102": ALTO_VALOR,
            "TXN000103": ALTO_VALOR,
            "TXN000204": TEMPO_60S,
            "TXN000302": GEO_10M,
        }
        saida = StringIO()

        for sequencia in sequencias:
            self.detector = DetectorFraudes()
            for transacao in sequencia:
                with redirect_stdout(saida):
                    resultado = self.enviar(transacao.para_dict())
                    consumer.exibir_resultado(resultado)

        for tipo in (ALTO_VALOR, TEMPO_60S, GEO_10M):
            self.assertIn(f"ALERTA DE FRAUDE: {tipo}", saida.getvalue())

        linhas = self.conexao.execute(
            """
            SELECT t.transaction_id, a.fraud_type
            FROM alertas a
            JOIN transacoes t ON t.id = a.transacao_id
            """
        ).fetchall()
        alertas_gravados = {linha["transaction_id"]: linha["fraud_type"] for linha in linhas}

        self.assertEqual(alertas_gravados, alertas_esperados)
        self.assertEqual(database.resumo(self.conexao)["transacoes"], 9)
        self.assertEqual(database.resumo(self.conexao)["transacoes_fraudulentas"], 4)

    def test_mensagem_repetida_nao_duplica_nem_repete_alerta(self):
                                                                                  
        dados = tx(1, "CLI001", 12500, "Fortaleza", 0).para_dict()
        self.assertEqual(self.enviar(dados).status, consumer.PROCESSADA)
        self.assertEqual(self.enviar(dados).status, consumer.DUPLICADA)
        self.assertEqual(database.resumo(self.conexao)["transacoes"], 1)
        self.assertEqual(database.resumo(self.conexao)["alertas"], 1)

    def test_mensagem_duplicada_nao_conta_para_janela_de_tempo(self):
                                                                                      
        dados = tx(1, "CLI001", 50, "Crato", 0).para_dict()
        for _ in range(4):
            self.enviar(dados)
        self.assertEqual(database.resumo(self.conexao)["alertas"], 0)

    def test_mensagens_invalidas_vao_para_auditoria(self):
                                                                      
        r1 = consumer.processar_mensagem(self.conexao, self.detector, b"isso nao e json")
        r2 = self.enviar({"transaction_id": "TXN1"})
        r3 = consumer.processar_mensagem(self.conexao, self.detector, b"\xff\xfe")
        self.assertEqual([r.status for r in (r1, r2, r3)], [consumer.INVALIDA] * 3)
        self.assertEqual(database.resumo(self.conexao)["mensagens_invalidas"], 3)
        self.assertEqual(database.resumo(self.conexao)["transacoes"], 0)

    def test_dados_nao_sao_apagados_ao_reabrir_o_banco(self):
                                                       
        self.enviar(tx(1, "CLI001", 150, "Fortaleza", 0).para_dict())
        self.conexao.close()
        self.conexao = database.conectar(self.caminho)
        self.assertEqual(database.resumo(self.conexao)["transacoes"], 1)

    def test_historico_recuperado_apos_reiniciar(self):
                                                                                   
        self.enviar(tx(1, "CLI001", 200, "Fortaleza", 0).para_dict())
        novo_detector = DetectorFraudes()
        novo_detector.carregar_historico(database.carregar_historico_recente(self.conexao))
        fraudes = novo_detector.avaliar(tx(2, "CLI001", 200, "Sobral", 120))
        self.assertEqual(fraudes, [GEO_10M])

    def test_erro_do_banco_desfaz_tudo(self):
                                                                            
        self.conexao.execute("DROP TABLE alertas")
        self.conexao.commit()
        with self.assertRaises(sqlite3.Error):
            self.enviar(tx(1, "CLI001", 12500, "Fortaleza", 0).para_dict())
        self.assertEqual(self.conexao.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0], 0)
                                                           
        self.assertEqual(self.detector.avaliar(tx(2, "CLI001", 1, "Sobral", 5)), [])

    def test_formatacao_em_reais(self):
                                                
        self.assertEqual(formatar_reais(12500), "R$ 12.500,00")
        self.assertEqual(formatar_reais(150.5), "R$ 150,50")


class _Resultado(unittest.TextTestResult):
    def _nome(self, teste) -> str:
        return teste.shortDescription() or teste.id().split(".")[-1]

    def addSuccess(self, teste):
        super().addSuccess(teste)
        self.stream.writeln(f"[APROVADO]  {self._nome(teste)}")

    def addFailure(self, teste, err):
        super().addFailure(teste, err)
        self.stream.writeln(f"[REPROVADO] {self._nome(teste)}")

    def addError(self, teste, err):
        super().addError(teste, err)
        self.stream.writeln(f"[REPROVADO] {self._nome(teste)} (erro)")


if __name__ == "__main__":
    for fluxo in (sys.stdout, sys.stderr):
        try:
            fluxo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    resultado = unittest.TextTestRunner(resultclass=_Resultado, verbosity=0).run(suite)
    print()
    print("=" * 60)
    if resultado.wasSuccessful():
        print(f"RESULTADO: TODOS OS {resultado.testsRun} TESTES APROVADOS")
    else:
        print(f"RESULTADO: REPROVADO ({len(resultado.failures)} falhas, {len(resultado.errors)} erros)")
    print("=" * 60)
    sys.exit(0 if resultado.wasSuccessful() else 1)
