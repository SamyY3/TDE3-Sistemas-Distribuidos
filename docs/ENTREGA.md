# Checklist da entrega e roteiro de apresentação

## 1. Exigência do professor → onde está / o que comprova

| # | Exigência | Arquivo | Como comprovar |
|---|---|---|---|
| 1 | Kafka de verdade (sem fila simulada) | `compose.yaml`, `fraud_kafka/producer.py`, `fraud_kafka/consumer.py` | `docker compose ps` (healthy) e `kafka-console-consumer.sh` mostrando as mensagens |
| 2 | Produtor: 1 transação a cada 3 s no tópico `transacoes` | `fraud_kafka/producer.py` (`--intervalo`, padrão 3 s) | console do produtor: linhas `PUBLICADA ...` espaçadas de 3 s |
| 3 | Clientes CLI001..., valores/cidades variados, IDs únicos, data/hora | `fraud_kafka/producer.py` (`GeradorAleatorio`, `GeradorId`) | saída do produtor; `SELECT COUNT(*), COUNT(DISTINCT transaction_id) FROM transacoes` |
| 4 | Normais e fraudulentas | `fraud_kafka/producer.py` (~70 % normais, resto dispara regras) | `python -m fraud_kafka.consultar_db` |
| 5 | JSON no formato padrão e fuso documentado | `fraud_kafka/fraud_detector.py` (`validar_transacao`), `fraud_kafka/config.py`, README | testes de validação de timestamp e transação |
| 6 | Ctrl+C encerra de forma controlada (produtor e consumidor) | `fraud_kafka/producer.py`, `fraud_kafka/consumer.py` | mensagem "Encerrado." ao apertar Ctrl+C |
| 7 | Consumidor lê o tópico continuamente e valida campos/tipos | `fraud_kafka/consumer.py`, `fraud_kafka/fraud_detector.py` | testes de campos ausentes, tipos errados e timestamps inválidos |
| 8 | Salva TODAS as transações válidas (normais e fraudes) | `fraud_kafka/database.py` (`salvar_transacao`) | teste "Todas as transacoes validas sao salvas..." |
| 9 | Alerta claro + tipo de fraude no console | `fraud_kafka/consumer.py` (`exibir_resultado`) | `!!! ALERTA DE FRAUDE: ALTO_VALOR ...` |
| 10 | Não perder transações / tratar erros Kafka e banco | `fraud_kafka/consumer.py` (commit após gravar, retry, `seek`), `fraud_kafka/config.py` | teste "Falha no meio da gravacao faz ROLLBACK" |
| 11 | Regra 1 ALTO_VALOR (`>= 10000`, inclui exatos) | `fraud_kafka/fraud_detector.py` | testes 2, 3, 3b |
| 12 | Regra 2 TEMPO_60s (4 em < 60 s, por cliente, janela móvel) | `fraud_kafka/fraud_detector.py` | testes 4, 4b, 5, 5b, 5c, 8, 8b |
| 13 | Regra 3 GEO_10m (cidades diferentes em até 10 min, por cliente) | `fraud_kafka/fraud_detector.py` | testes 6, 6b, 7, 7b, 7c, 8 |
| 14 | Múltiplas fraudes na mesma transação | `fraud_kafka/fraud_detector.py`, `fraud_kafka/database.py` | teste 9 e "Alertas ficam no banco... todos os tipos" |
| 15 | Cenários controlados separados do produtor aleatório | `fraud_kafka/cenarios.py` (+ `fraud_kafka/producer.py --cenarios`) | testes dos cenários |
| 16 | Os 9 casos pedidos | `tests/test_fraud_detector.py` | testes numerados 1 a 9 |
| 17 | Testes automáticos com aprovação/reprovação, sem esperar 3 s | `tests/test_fraud_detector.py` | `python -m tests.test_fraud_detector` |
| 18 | SQLite criado automaticamente, sem apagar dados ao reiniciar | `fraud_kafka/database.py` | teste "Reiniciar nao apaga transacoes anteriores" |
| 19 | Tabela de transações e tabela de alertas vinculada | `fraud_kafka/database.py` | `SELECT * FROM vw_fraudes;` |
| 20 | Sem duplicar a mesma mensagem | `fraud_kafka/database.py` (`UNIQUE`), `fraud_kafka/consumer.py` | teste "Mesma mensagem processada de novo..." |
| 21 | SQL com transação e tratamento de erro | `fraud_kafka/database.py` (`with conexao:`) | teste de rollback |
| 22 | Como consultar o banco no Windows/VS Code | `README.md`, `fraud_kafka/consultar_db.py` | — |
| 23 | Estrutura de pastas e `.gitignore` | raiz do projeto | `git status` não mostra `.db` |
| 24 | Instruções Windows 11 / PowerShell | `README.md` (instalação e execução) | — |
| 25 | Git/GitHub com ≥ 4 commits | histórico do repositório | `git log --oneline` |
| 26 | Documentação do projeto| `README.md` | — |

## 2. Roteiro de apresentação (explicação simples)

**Ideia geral (30 s):** "Quando alguém faz uma transação no banco, o sistema precisa decidir na hora se ela é
suspeita. Eu montei um sistema com Apache Kafka que recebe as transações, aplica 3 regras de fraude e guarda
tudo em um banco de dados."

**Papéis de cada parte:**

- **Produtor (`fraud_kafka/producer.py`)**: é como o caixa eletrônico/app do banco. A cada 3 segundos cria uma transação
  (cliente, valor, cidade, horário) e manda para o Kafka. Ele não sabe quem vai ler.
- **Kafka**: é o "correio" no meio. Guarda as mensagens no tópico `transacoes` na ordem em que chegam e entrega
  para quem quiser ler. Se o consumidor cair, as mensagens esperam no Kafka. Roda em Docker.
- **Consumidor (`fraud_kafka/consumer.py`)**: é o analista de fraudes. Lê cada mensagem, confere se está certa, aplica as
  regras, grava no banco e mostra o alerta na tela. Só avisa o Kafka "já li" depois de gravar, para não perder nada.
- **Banco SQLite (`fraud_kafka/database.py`)**: é o arquivo de registro. Guarda **todas** as transações (normais e
  fraudulentas) e uma tabela de alertas ligada à transação suspeita. Não aceita a mesma transação duas vezes.

**As 3 regras (exemplos para falar):**

1. **ALTO_VALOR**: valor ≥ R$ 10.000,00 (o valor exato 10.000 já conta). Ex.: CLI001 com R$ 12.500,00.
2. **TEMPO_60s**: o mesmo cliente faz 4 transações em menos de 60 s. Olho as 4 últimas dele; se a diferença
   entre a primeira e a quarta for menor que 60 s, é fraude (exatamente 60 s não é).
3. **GEO_10m**: o mesmo cliente aparece em duas cidades diferentes em até 10 min (ex.: Fortaleza e Sobral com
   5 min de diferença: não dá para estar nos dois lugares).

**Demonstração ao vivo (2-3 min):**

1. No VS Code, execute a tarefa `Kafka: iniciar (Docker Compose)` e confira `docker compose ps` até o serviço ficar saudável.
2. `.\.venv\Scripts\python.exe -m tests.test_fraud_detector` → conferir os testes locais sem Kafka.
3. Terminal 1: `.\.venv\Scripts\python.exe -u -m fraud_kafka.consumer`. Terminal 2: `.\.venv\Scripts\python.exe -u -m fraud_kafka.producer --cenarios` → mostrar os alertas.
4. `.\.venv\Scripts\python.exe -m fraud_kafka.consultar_db` (ou abrir o `.db` no VS Code) → mostrar transações normais e fraudes salvas.
5. Opcional: `.\.venv\Scripts\python.exe -u -m fraud_kafka.producer` para mostrar a geração aleatória a cada 3 s; `Ctrl+C` para encerrar.

**Perguntas que o professor pode fazer:**

- *Por que usar Kafka?* Desacopla quem gera de quem processa, guarda as mensagens e aguenta picos de volume.
- *E se o consumidor cair?* O Kafka mantém as mensagens; ao voltar, ele continua de onde parou (commit só depois de gravar).
- *E se a mesma mensagem chegar duas vezes?* O `transaction_id` é `UNIQUE` no banco; a repetida é ignorada e não gera alerta duplicado.
- *Como testou sem esperar 3 s?* As regras usam o horário que vem na transação, então os testes simulam os horários.
- *Qual o fuso horário?* Brasília (UTC-03:00), sem sufixo na string do timestamp.
