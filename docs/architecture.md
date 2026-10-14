# Decisões e invariantes

## Responsabilidades e consistência

`domain.py` define objetos, estados e transições. `application.py` coordena casos de uso e unidades de trabalho; `ports.py` declara os contratos. Adaptadores implementam persistência, mensageria e risco. A composição das dependências ocorre em `bootstrap.py`; processos e CLI são pontos de entrada.

SQLAlchemy utiliza mapeamento imperativo externo. O domínio não conhece sessões, tabelas, FastAPI ou Kafka. A unidade de trabalho controla commit/rollback e destaca objetos antes de encerrar a sessão. Cada iteração usa uma sessão própria; não há sessão compartilhada entre executores.

O MySQL é a fonte de verdade. A criação faz INSERT da transação e dos eventos de solicitação e status `PENDING` no mesmo commit. Cada transição pública incrementa `version` e registra seu evento de status. `generation` incrementa apenas no replay. O consumer lê os dados atuais do banco; o comando não transporta valores financeiros duplicados.

Atualizar status e publicar diretamente criaria uma janela de dual write. A outbox elimina a perda nesse intervalo, mas permite duplicação quando Kafka confirma e o processo cai antes de marcar a publicação. O produtor idempotente reduz duplicatas em sua sessão; não elimina duplicatas entre reinícios.

## Handoff Kafka → MySQL

O consumer desabilita auto commit e auto offset store. Para cada mensagem, valida o envelope e cria o trabalho sob lock da transação. Somente após o commit MySQL confirma `offset + 1` para aquela mensagem. Uma redelivery encontra `event_id` ou `(transaction_id, generation)` já registrados.

Se o banco falhar, o processo mantém a mensagem atual e não busca offsets posteriores. Essa escolha também bloqueia outras partições atribuídas àquela instância durante a falha. Em indisponibilidades maiores que `max.poll.interval.ms`, o grupo pode remover o membro; falha de commit encerra o processo e o restart retoma com deduplicação. Heartbeats do processo indicam vida, não ausência de backlog.

Mensagem inválida não pode prender uma partição indefinidamente: o consumer grava quarentena e evento DLQ atomicamente antes de confirmar o offset. Se essa gravação falhar, a mensagem permanece sem confirmação. O payload original fica em base64 no MySQL; a DLQ publica referência e motivo, sem replicar dados pessoais.

## Execução e recuperação

1. Buscar primeiro leases vencidos e depois trabalhos READY com prazo vencido. As consultas usam índices separados `(status, lease_until)` e `(status, next_attempt_at)` com `SKIP LOCKED`, evitando a ordenação de uma condição OR que poderia bloquear candidatos adicionais.
2. Sob transação curta, incrementar tentativa, atribuir token, reservar lease e mudar o status para `PROCESSING` quando necessário.
3. Confirmar e liberar locks antes da chamada externa.
4. Em outra transação, bloquear o job e verificar estado, token, lease e geração. Somente então aplicar o resultado.
5. Em falha temporária, persistir próximo horário e `RETRY_PENDING`; em exaustão ou erro definitivo, persistir `FAILED`, falha e eventos de status/DLQ no mesmo commit.

Cada reserva consome uma tentativa, inclusive quando o executor cai antes ou durante a chamada. O crash na última tentativa é convertido em falha final após expiração do lease. Uma chamada concluída após perder o lease é descartada localmente; o efeito externo pode ter ocorrido, portanto o contrato HTTP usa `Idempotency-Key: transaction_id:generation`.

HTTP 408, 429, 5xx, erros de transporte e deadline são temporários. Outros códigos e respostas fora de `APPROVED/REJECTED` são erros definitivos de contrato. Não se aplica retry a uma rejeição de negócio.

O backoff é `min(10 × 2^(tentativa−1), 300)` segundos, mais jitter uniforme de 0 a 20%. Com 16 tentativas, a janela mínima de agendamento é aproximadamente 55 minutos, além dos tempos de execução. Depois de uma indisponibilidade de 30 minutos ainda existem tentativas disponíveis. O orçamento é configurável e finito: não promete recuperação automática de uma indisponibilidade sem limite.

## Eventos, ordem e versões

O envelope JSON contém `event_id`, `event_type`, `schema_version`, `transaction_id`, `generation`, `aggregate_version`, `occurred_at`, `correlation_id` e `data`. IDs são UUIDs e timestamps incluem timezone. Campos extras são tolerados; campos obrigatórios e versão são validados.

| Tópico | Tipo |
|---|---|
| `transactions.commands.v1` | `transaction.processing_requested` |
| `transactions.status.v1` | `transaction.status_changed` |
| `transactions.dlq.v1` | `transaction.processing_failed` ou `message.quarantined` |

Exemplo de comando:

```json
{
  "event_id": "11111111-1111-4111-8111-111111111111",
  "event_type": "transaction.processing_requested",
  "schema_version": 1,
  "transaction_id": "22222222-2222-4222-8222-222222222222",
  "generation": 1,
  "aggregate_version": 1,
  "occurred_at": "2026-10-13T12:00:00+00:00",
  "correlation_id": "33333333-3333-4333-8333-333333333333",
  "data": {}
}
```

O publicador processa a menor ID pendente e segura o lock até confirmação Kafka e commit SQL. O ambiente prevê uma réplica desse processo. Chave Kafka `transaction_id` mantém afinidade de partição; quarentena sem transação usa `event_id`. A ordem é por chave e tópico, sem garantia de ordem global entre tópicos. Consumers de status devem deduplicar `event_id` e rejeitar versões menores ou iguais à versão aplicada.

Mudanças aditivas opcionais mantêm versão 1. Mudanças incompatíveis exigem versão/tópico novos, atualização dos consumidores e migração explícita. Versões desconhecidas são colocadas em quarentena. Alterar quantidade de partições pode mudar o particionamento de uma chave; requer planejamento para preservar ordem durante a transição.

Replay de transação exige `FAILED`, cria geração nova e mantém os jobs anteriores. Replay de quarentena exige arquivo corrigido, contrato válido, transação existente em `PENDING`, geração/versão atuais e nenhum job dessa geração. Não cria transações a partir de mensagens arbitrárias. O histórico registra horário e evento do replay.

## Escala e operação de produção

10.000 eventos/minuto equivalem a cerca de 167 eventos/segundo, mas cada transação gera vários eventos e commits. A quantidade real no Kafka depende do número de transições e retries. Dimensionar apenas a taxa de entrada subestima a carga da outbox e do MySQL.

Como aproximação, concorrência de chamadas deve superar taxa de chegada × latência média, com margem para picos e erros. A 167 transações/segundo e 100 ms por análise seriam necessárias pelo menos 17 chamadas simultâneas, antes da margem. Isso é dimensionamento teórico, não resultado de benchmark.

Escalar API por CPU/latência, consumers por lag e partições, executores por idade dos trabalhos e capacidade do risco. Usar índices, pools limitados e restrições de conexão; evitar multiplicar réplicas sem calcular o orçamento de conexões MySQL. A fila MySQL permite escalar execução independentemente das seis partições Kafka, com maior carga no banco como contrapartida.

Evoluções: relay particionado por chave ou CDC/outbox, publicação em lotes, cluster Kafka com replicação 3 e `min.insync.replicas=2`, MySQL com HA/backups testados, métricas e tracing. Monitorar idade da outbox, atraso de ingestão, idade dos jobs, latência/erros do risco e DLQ. Definir retenção dos eventos, jobs e falhas junto à janela de recuperação; remover deduplicação antes dessa janela pode reintroduzir trabalho antigo.

## Referências técnicas

- [SQLAlchemy: sessões e transações](https://docs.sqlalchemy.org/en/20/orm/session_basics.html).
- [MySQL: locking reads e SKIP LOCKED](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html).
- [Cliente Python Kafka: commits e garantias](https://docs.confluent.io/kafka-clients/python/current/overview.html).
- [Imagem Docker oficial Kafka](https://kafka.apache.org/41/getting-started/docker/).
