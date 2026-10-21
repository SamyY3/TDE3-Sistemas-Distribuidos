# FraudKafka

Sistema acadêmico de detecção de fraudes em transações financeiras, desenvolvido
em Python com Apache Kafka e SQLite para o TDE 03 de Sistemas Distribuídos.

O produtor publica transações simuladas no Kafka; o consumidor valida cada
mensagem, aplica regras de detecção e persiste os resultados. O sistema registra
todas as transações válidas — normais ou suspeitas — e mantém cada alerta
associado à transação correspondente.

## Funcionalidades

- Publicação e consumo de transações JSON pelo tópico `transacoes`.
- Detecção de alto valor, frequência elevada e mudança de localização.
- Persistência em SQLite, com prevenção de duplicatas e auditoria de mensagens
  inválidas.
- Cenários determinísticos para demonstrações e testes automatizados sem Kafka.
- Cliente Kafka configurável e reutilizável.

## Estrutura do projeto

```text
.
├── docs/
│   └── ENTREGA.md
├── fraud_kafka/
│   ├── cenarios.py
│   ├── config.py
│   ├── consultar_db.pyrode 
│   ├── consumer.py
│   ├── database.py
│   ├── fraud_detector.py
│   ├── kafka_client.py
│   └── producer.py
├── tests/
│   ├── test_fraud_detector.py
│   └── test_kafka_client.py
├── compose.yaml
├── requirements.txt
└── README.md
```

O banco SQLite e o contador do produtor são criados em `data/` na primeira
execução. Essa pasta contém dados locais e não deve ser versionada.

## Regras de detecção

| Regra | Condição |
|-------|----------|
| `ALTO_VALOR` | Valor igual ou superior a R$ 10.000,00. |
| `TEMPO_60s` | Quatro transações do mesmo cliente em menos de 60 segundos. |
| `GEO_10m` | Transações do mesmo cliente em cidades diferentes em até 10 minutos. |

As janelas usam o horário informado na transação. Uma única transação pode
acionar mais de uma regra.

## Requisitos

- Python 3.10 ou superior.
- Docker com Docker Compose (Docker Desktop no Windows/macOS ou Docker Engine
  com o plugin Compose no Linux), para executar o Kafka local.

## Instalação no Windows / PowerShell

Na raiz do projeto:

```powershell
py -m venv .venv

.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Se o PowerShell bloquear a ativação do ambiente virtual, consulte a política de
execução configurada na sua máquina ou execute os comandos usando o Python em
`.venv\Scripts\python.exe`.

## Instalação no Linux / macOS

Na raiz do projeto, crie e ative o ambiente virtual e instale as dependências:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

No Linux, instale o Docker Engine e o plugin Docker Compose. No macOS, instale e
inicie o Docker Desktop. Confirme que `docker compose version` funciona antes
de iniciar o Kafka.

## Como executar pelo terminal integrado do VS Code

Abra a pasta do projeto no VS Code e selecione **Terminal > Novo Terminal**.
Os comandos desta seção para Windows pressupõem que o terminal seja o
PowerShell e esteja aberto na pasta principal do projeto.

### 1. Verifique o Python e prepare o ambiente

Confira se o Python está instalado:

```powershell
py --version
```

Se o ambiente virtual `.venv` ainda não existir, crie-o:

```powershell
py -m venv .venv
```

Instale as dependências do projeto:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Se o ambiente virtual e as dependências já estiverem prontos, continue para a
próxima etapa.

### 2. Inicie o Kafka

O projeto inclui a tarefa **Kafka: iniciar (Docker Compose)**. Para usá-la,
pressione **Ctrl + Shift + P**, escolha **Tasks: Run Task** e selecione essa
tarefa. Como alternativa, inicie o Kafka pelo terminal:

```powershell
docker compose up -d
docker compose ps
```

O serviço `kafka` deve aparecer como iniciado/saudável. Para parar o broker
iniciado por Compose, use a tarefa **Kafka: parar (Docker Compose)** ou execute
`docker compose down`.

Se o Docker não estiver disponível neste computador, é possível usar um broker
Kafka nativo **desde que ele já esteja instalado, configurado e em execução**,
ouvindo em `localhost:9092`. O projeto não inclui uma tarefa do VS Code chamada
**Kafka nativo** nem instala/configura um broker nativo. Inicie-o usando o
procedimento correspondente à sua instalação do Kafka. Se o broker usar outro
endereço, configure a variável `KAFKA_BOOTSTRAP`, descrita na seção
Configuração.

### 3. Verifique a conexão com o Kafka

Abra outro terminal no VS Code e, no PowerShell, execute:

```powershell
Test-NetConnection localhost -Port 9092
```

Confirme que o resultado contém:

```text
TcpTestSucceeded : True
```

Se aparecer `False`, confirme se o broker está em execução e ouvindo na porta
9092. Esse teste verifica apenas a conexão TCP com a porta; não confirma, por si
só, que o Kafka está funcionando corretamente.

### 4. Inicie o consumidor

Em um terminal, execute o consumidor como módulo Python:

```powershell
.\.venv\Scripts\python.exe -u -m fraud_kafka.consumer
```

Ele conecta ao Kafka, prepara o banco SQLite e aguarda transações. Deixe esse
terminal aberto.

### 5. Publique transações com o produtor

Abra outro terminal e escolha uma das opções:

**Enviar cinco transações e encerrar:**

```powershell
.\.venv\Scripts\python.exe -u -m fraud_kafka.producer --max 5
```

**Publicar transações aleatórias continuamente** (uma a cada três segundos):

```powershell
.\.venv\Scripts\python.exe -u -m fraud_kafka.producer
```

**Executar os cenários de demonstração das regras de fraude:**

```powershell
.\.venv\Scripts\python.exe -u -m fraud_kafka.producer --cenarios
```

Os cenários são determinísticos: os horários das transações são simulados para
demonstrar as regras temporais sem esperar 60 segundos ou 10 minutos. Observe o
terminal do consumidor para acompanhar o processamento e os alertas.

O produtor também aceita `--intervalo 1.5` para alterar o intervalo entre
publicações e `--max 20` para limitar a quantidade de transações. Para parar um
produtor contínuo ou o consumidor, pressione **Ctrl + C** no terminal
correspondente.

### 6. Consulte os dados gravados

Depois que o consumidor processar as transações, abra outro terminal e execute:

```powershell
.\.venv\Scripts\python.exe -m fraud_kafka.consultar_db
```

Para exibir até 50 registros por tabela:

```powershell
.\.venv\Scripts\python.exe -m fraud_kafka.consultar_db --limite 50
```

O banco fica em `data/transacoes.db`. São salvas todas as transações válidas,
fraudulentas ou não, e os alertas correspondentes.

### 7. Execute os testes

Os testes automatizados não precisam de uma conexão ativa com Kafka nem de
Docker. Para executar todos:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```

Para executar especificamente os testes das regras e cenários de fraude:

```powershell
.\.venv\Scripts\python.exe -m tests.test_fraud_detector
```

### Linux e macOS

No Linux/macOS, os mesmos módulos podem ser executados com o ambiente virtual
ativado. Inicie o Kafka por Docker Compose (`docker compose up -d`) ou use um
broker nativo configurado para estar acessível no endereço indicado por
`KAFKA_BOOTSTRAP`. Em terminais separados, execute:

```bash
python -u -m fraud_kafka.consumer
```

Em outro terminal, inicie o produtor:

```bash
python -u -m fraud_kafka.producer --max 5
```

Para o produtor contínuo, omita `--max 5`; para os cenários de demonstração,
use `python -u -m fraud_kafka.producer --cenarios`. Para consultar o banco,
execute `python -m fraud_kafka.consultar_db`. Os testes podem ser executados com
`python -m unittest discover -v`.

## Configuração

As configurações abaixo são opcionais:

| Variável | Padrão | Uso |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Endereço do broker Kafka. |
| `KAFKA_TOPICO` | `transacoes` | Tópico de publicação e consumo. |
| `KAFKA_GRUPO` | `detector-fraudes` | Grupo do consumidor. |
| `FRAUDES_DB` | `data/transacoes.db` | Caminho alternativo para o banco SQLite. |

Exemplo no PowerShell:

```powershell
$env:KAFKA_BOOTSTRAP = "localhost:9092"
$env:KAFKA_TOPICO = "eventos"
$env:KAFKA_GRUPO = "meu-grupo"
```

No Linux/macOS, configure as variáveis no terminal antes de iniciar o programa:

```bash
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_TOPICO="eventos"
export KAFKA_GRUPO="meu-grupo"
```

Os timestamps são tratados como horário de Brasília (UTC-03:00) e serializados
no formato ISO 8601 sem sufixo de fuso, por exemplo
`2026-10-20T14:30:00`.

## Cliente Kafka reutilizável

O cliente está em [`fraud_kafka/kafka_client.py`](./fraud_kafka/kafka_client.py).
Em outro projeto Python, copie o pacote `fraud_kafka/`, instale `kafka-python` e
reutilize `KafkaClient` para criar produtores e consumidores ou garantir a
existência de um tópico. O broker precisa estar em execução e acessível.

```python
from fraud_kafka.kafka_client import KafkaClient

kafka = KafkaClient()
kafka.ensure_topic()
producer = kafka.producer()
producer.send(
    kafka.topic,
    key="cliente-1",
    value={"mensagem": "Olá, Kafka"},
).get(timeout=10)
producer.close()
```

## Documentação da entrega

O checklist dos requisitos acadêmicos e o roteiro de apresentação estão em
[`docs/ENTREGA.md`](./docs/ENTREGA.md).
