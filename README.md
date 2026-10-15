# BAMAQ Capital

Aplicação para receber e analisar transações financeiras de forma assíncrona. Implementa o case com Python 3.13, FastAPI, SQLAlchemy 2.0, MySQL 8.4 e Kafka KRaft, priorizando consistência, recuperação de falhas e clareza das responsabilidades.

## Executar

Pré-requisito: Docker com Compose. As portas locais 8000, 8001, 8080, 3306 e 9092 devem estar disponíveis. Execute os comandos na raiz deste repositório.

```sh
docker compose up -d --build
docker compose ps
```

O Compose cria os bancos, aplica migrações Alembic e inicializa os três tópicos com seis partições. API, consumer, executores e publicador usam a mesma imagem, com comandos independentes. O primeiro build precisa baixar imagens e dependências.

- API e Swagger: [http://localhost:8000/docs](http://localhost:8000/docs).
- OpenAPI: [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json).
- Mock de risco: [http://localhost:8001/docs](http://localhost:8001/docs).
- Prontidão da API: [http://localhost:8000/health/ready](http://localhost:8000/health/ready).
- Administração web do MySQL (Adminer): [http://localhost:8080](http://localhost:8080).

Verificação de ponta a ponta, com aprovação e rejeição reais pelo fluxo Kafka:

```sh
docker compose exec api python scripts/smoke.py
```

Para encerrar preservando os dados: `docker compose down`. Os volumes MySQL e Kafka persistem entre execuções. As credenciais do Compose são exclusivas do ambiente local do desafio.

## Administração web do MySQL

O Compose inclui [Adminer](https://hub.docker.com/_/adminer), com versão fixada, disponível em [http://localhost:8080](http://localhost:8080). Para iniciar somente a interface e sua dependência MySQL:

```sh
docker compose up -d adminer
```

Na tela de login, informe:

| Campo | Valor |
|---|---|
| Sistema | `MySQL` |
| Servidor | `mysql` (preenchido automaticamente) |
| Usuário | `bamaq` |
| Senha | `bamaq` |
| Banco de dados | `bamaq` |

Depois de entrar, selecione `transactions` ou `outbox_events` e clique em **Selecionar dados** para visualizar e filtrar registros. A opção **Comando SQL** permite executar consultas, por exemplo:

```sql
SELECT * FROM transactions ORDER BY created_at DESC LIMIT 20;
SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY id;
```

O Adminer está conectado ao banco real da aplicação e permite alterar dados conforme as permissões do usuário. A interface é publicada apenas em `127.0.0.1`, para uso local. O banco `bamaq_test` é exclusivo dos testes automatizados.

## API

Criar uma transação, em Bash:

```sh
curl -i http://localhost:8000/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exemplo-001' \
  -d '{"customer_id":"123","value":"1500.00"}'
```

Em PowerShell:

```powershell
$transaction = Invoke-RestMethod -Method Post -Uri http://localhost:8000/transactions `
  -ContentType 'application/json' -Headers @{'Idempotency-Key'='exemplo-001'} `
  -Body '{"customer_id":"123","value":"1500.00"}'
Invoke-RestMethod "http://localhost:8000/transactions/$($transaction.id)"
```

O POST retorna `202 Accepted` e `Location: /transactions/{id}`. O GET retorna o estado atual, sem cache. Exemplo de resposta:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "customer_id": "123",
  "value": "1500.00",
  "status": "APPROVED"
}
```

Valores são em BRL, positivos, com até duas casas decimais e máximo `9999999999999999.99`. A entrada aceita número JSON ou string decimal; a resposta usa string com duas casas para preservar precisão. Para valores grandes, envie string. O domínio usa `Decimal` e o banco `DECIMAL(18,2)`.

`customer_id` deve ter de 1 a 128 caracteres, após remover espaços das extremidades. Campos extras são rejeitados. `Idempotency-Key` é opcional, aceita de 1 a 128 caracteres ASCII visíveis sem espaços e é sensível a maiúsculas. Mesma chave e conteúdo normalizado retornam a transação original; outra combinação retorna `409`. Sem chave, dois POSTs representam transações diferentes.

| Resposta | Significado |
|---|---|
| `202` | Transação persistida; publicação e análise são assíncronas |
| `200` | Consulta encontrada |
| `404` | Identificador válido, transação inexistente |
| `409` | Conflito de idempotência |
| `422` | Entrada ou UUID inválido |
| `503` | Persistência temporariamente indisponível |

Estados: `PENDING → PROCESSING → APPROVED / REJECTED`; falhas temporárias usam `PROCESSING → RETRY_PENDING → PROCESSING`; falha técnica definitiva usa `FAILED`. Replay autorizado pela CLI faz `FAILED → PENDING` em uma nova geração.

## Arquitetura e decisões

![Arquitetura BAMAQ Capital](docs/architecture.svg)

[Desenho SVG](docs/architecture.svg) · [Fonte Mermaid](docs/architecture.mmd) · [Detalhamento técnico](docs/architecture.md) · [Operação e recuperação](docs/operations.md)

- **Arquitetura hexagonal:** domínio sem imports de frameworks; casos de uso dependem de portas. SQLAlchemy faz mapeamento externo dos objetos de domínio. Kafka, HTTP, FastAPI e CLI são adaptadores.
- **Outbox transacional:** criação e alterações públicas de estado gravam os respectivos eventos no mesmo commit MySQL. A API funciona durante falha Kafka; o backlog aguarda recuperação.
- **Trabalho durável:** o consumer persiste o job antes de confirmar o offset. A análise externa ocorre em outro processo, evitando prender a partição a uma dependência lenta. O custo é uma fila adicional no MySQL e mais operações SQL.
- **Idempotência:** restrições únicas por evento e por transação/geração. A tabela de trabalhos também é o registro de deduplicação; retenção desses registros deve acompanhar a janela de replay.
- **Retries:** até 16 tentativas totais, backoff de 10 segundos até cinco minutos, com jitter de até 20%. Nenhum worker dorme segurando um job entre tentativas. A configuração padrão permite recuperação após 30 minutos de indisponibilidade.
- **Leases:** posse por 30 segundos, recuperada após expiração. Token e geração impedem que um executor atrasado sobrescreva resultados. A chamada HTTP tem deadline total de cinco segundos e ocorre fora da transação SQL.
- **DLQ e replay:** falhas finais e mensagens inválidas são registradas no MySQL e publicadas na DLQ por outbox. Reprocessamento preserva histórico; resultados de negócio concluídos não são reabertos.
- **Redis:** omitido porque não é necessário para as garantias; consultas diretas evitam invalidação de cache e outro componente operacional.

A garantia é entrega **pelo menos uma vez**, com efeitos locais idempotentes. A chamada HTTP pode se repetir após um crash: o mock recebe chave estável por transação e geração. Isso não equivale a uma transação distribuída ou a execução externa exatamente uma vez.

| Cenário do case | Comportamento |
|---|---|
| MySQL confirma e Kafka falha | Evento fica na outbox, sem perder a transação |
| Processo morre após persistir e antes do commit Kafka | Redelivery encontra o mesmo trabalho e não duplica o efeito |
| Risco indisponível por 30 minutos | Retry durável, sem bloquear o consumer; análise retoma após recuperação |
| Mensagem duplicada | Deduplicação por evento e geração |
| 100 para 10.000 eventos/minuto | Escala independente, filas absorvem picos; dimensionamento depende de benchmark |

## Testes

Suíte completa com MySQL e Kafka reais, usando banco e tópicos isolados:

```sh
docker compose --profile test run --rm tests
```

O banco `bamaq_test` é criado na primeira inicialização do volume MySQL. Testes de integração limpam somente tabelas desse banco; uma proteção exige nome terminado em `_test`. Testes Kafka criam e removem tópicos próprios. Não execute duas suítes simultaneamente sobre o mesmo banco de testes.

Para desenvolvimento local, use Python 3.13:

```sh
python -m venv .venv
# Bash: source .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.lock
python -m pip install --no-deps --no-build-isolation -e .
pytest -q -m "not integration"
ruff check src tests migrations scripts
ruff format --check src tests migrations scripts
```

Para integração fora do container, inicie `docker compose up -d mysql kafka` e configure:

```powershell
$env:BAMAQ_TEST_DATABASE_URL = 'mysql+pymysql://bamaq:bamaq@localhost:3306/bamaq_test?charset=utf8mb4'
$env:BAMAQ_TEST_KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
pytest -q --cov=bamaq --cov-report=term-missing
```

Em Bash, use `export VAR=valor` para essas duas variáveis. Sem elas, os testes correspondentes são explicitamente ignorados.

A suíte cobre sucesso/rejeição, precisão decimal, validação, idempotência de POST, rollback, concorrência real de consumers e executores, `SKIP LOCKED`, recuperação de lease e fencing, retry, exaustão, replay, quarentena, evolução de eventos e indisponibilidade Kafka. O teste de 30 minutos usa relógio controlável. Um teste Kafka fecha o consumer após persistir e antes de confirmar o offset, reinicia o grupo e verifica a deduplicação.

O workflow em `.github/workflows/ci.yaml` executa lint, testes com infraestrutura real, smoke test e verificação das migrações. [Registro de validação local](docs/validation.md).

## Teste de carga local

**Execução opcional:** o teste de carga é executado manualmente e não é necessário para iniciar ou utilizar a aplicação.

O gerador oferece taxas progressivas de POSTs e verifica no MySQL a conclusão das transações, a drenagem da outbox e os resultados de negócio. Pelo Compose, também mede o atraso do grupo Kafka. Cada execução gera relatórios JSON/CSV em `reports/load-tests/load-UUID/`.

```sh
docker compose up -d --build --wait
# Execução pequena: 150 transações, com taxas de 5 e 10 transações/segundo.
docker compose --profile load run --rm load-test
# Cenário progressivo até aproximadamente 10.000 transações recebidas/minuto.
docker compose --profile load run --rm load-test --case
```

O cenário `--case` cria 25.290 transações e oferece até 167 POSTs/segundo por dois minutos. Isso representa cerca de 40.000 eventos Kafka/minuto no estágio final, pois cada transação sem retry produz quatro eventos. O teste pequeno é a execução padrão; o benchmark não roda automaticamente no CI.

As transações sintéticas são gravadas no banco `bamaq` e permanecem identificadas pelo `customer_id` e pelo prefixo `Idempotency-Key` `load-UUID`. O teste não apaga dados. Um código de saída 0 significa que as verificações e os limites configurados passaram; 1 indica falhas verificadas; 2 indica erro de execução/configuração.

Consulte [Teste de carga: comandos, métricas e interpretação](docs/load-testing.md) para configurar estágios, limites de latência, comparar réplicas e interpretar filas crescentes. Implementar o gerador não comprova capacidade de 10.000 eventos/minuto: essa conclusão depende do relatório de uma execução sustentada no ambiente medido.

## Configuração e processos

As variáveis começam com `BAMAQ_`; veja [.env.example](.env.example). Copiar esse arquivo para `.env` é opcional. O Compose define endereços internos; URLs do exemplo são para execução no host.

| Comando | Responsabilidade |
|---|---|
| `bamaq api` | API na porta 8000 |
| `bamaq risk-mock` | Mock na porta 8000; Compose publica como 8001 |
| `bamaq relay` | Publicação ordenada da outbox |
| `bamaq consumer` | Ingestão durável dos comandos Kafka |
| `bamaq worker` | Análise e recuperação de trabalhos |
| `bamaq failures` | Inspeção de metadados das falhas |
| `bamaq replay UUID` | Nova geração de transação `FAILED` |
| `bamaq replay-quarantine UUID --payload-file arquivo.json` | Reenvio de comando corrigido e validado |

`alembic upgrade head` aplica migrações. O mock aprova até BRL 5.000,00 e rejeita acima desse limite. Os parâmetros `BAMAQ_MOCK_MODE=unavailable/invalid`, `BAMAQ_MOCK_FAIL_FIRST=N` e `BAMAQ_MOCK_DELAY_SECONDS=N` simulam falhas e lentidão. O modo padrão é `normal`.

## Limitações e evolução

O ambiente possui um broker e um MySQL, sem alta disponibilidade. O publicador é deliberadamente único e espera confirmação de cada evento: mantém simplicidade e ordem, mas pode limitar vazão. Antes de produção, medir carga, paralelizar publicação por chave e configurar replicação Kafka, TLS/SASL, autenticação/autorização da API, segredos externos e alta disponibilidade MySQL.

Os executores podem ser escalados com `docker compose up -d --scale worker=4`. Consumers podem ser escalados até o número de partições; são limitados pela ingestão, enquanto executores são limitados pela análise externa e pelo banco. Leases dependem de relógios sincronizados. A idempotência do mock reside em memória e se perde ao reiniciar; um serviço real com efeitos financeiros precisa de deduplicação durável própria.

Logs JSON, health checks e um teste de carga local reproduzível estão implementados. Métricas Prometheus, tracing, circuit breaker compartilhado e retenção automatizada de históricos são evoluções. Não há alegação de capacidade de produção sem medição.
