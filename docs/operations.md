# Operação, falhas e reprocessamento

## Diagnóstico inicial

```sh
docker compose ps
docker compose logs --tail=100 api relay consumer worker
docker compose exec api bamaq failures --limit 20
```

Logs são JSON, com componente, transação, evento, geração, tentativa, correlação e duração quando aplicáveis. Dados do cliente, valor e payloads não são registrados. Exceções de infraestrutura são registradas pela classe, evitando parâmetros SQL em mensagens de erro. O Kafka/MySQL possuem seus próprios logs de infraestrutura.

`/health/live` indica que a API está respondendo; `/health/ready` testa MySQL. Kafka indisponível não remove a API de prontidão porque a outbox aceita trabalho durável. Health checks dos processos usam heartbeat local: a fila pode estar atrasada mesmo com processo saudável. Consultar backlog e erros é parte do diagnóstico.

## Consultas operacionais

Abra o cliente MySQL do ambiente local:

```sh
docker compose exec mysql mysql -ubamaq -pbamaq bamaq
```

```sql
-- Backlog e idade do evento mais antigo ainda não publicado.
SELECT COUNT(*) AS pending,
       TIMESTAMPDIFF(SECOND, MIN(created_at), UTC_TIMESTAMP(6)) AS oldest_seconds
FROM outbox_events WHERE published_at IS NULL;

-- Trabalhos e tentativas por estado.
SELECT status, COUNT(*) AS jobs, MAX(attempts) AS max_attempts
FROM processing_jobs GROUP BY status;

-- Retries disponíveis e atraso em relação ao agendamento.
SELECT COUNT(*) AS due_jobs,
       TIMESTAMPDIFF(SECOND, MIN(next_attempt_at), UTC_TIMESTAMP(6)) AS overdue_seconds
FROM processing_jobs
WHERE status = 'READY' AND next_attempt_at <= UTC_TIMESTAMP(6);

-- Leases vencidos que os executores devem recuperar.
SELECT id, transaction_id, generation, attempts, lease_until
FROM processing_jobs
WHERE status = 'RUNNING' AND lease_until <= UTC_TIMESTAMP(6);

-- Falhas recentes: metadados, sem payload original.
SELECT id, transaction_id, generation, reason, created_at, replayed_at
FROM failed_messages ORDER BY created_at DESC LIMIT 20;
```

Lag de ingestão Kafka:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9093 --describe --group bamaq-job-ingestion-v1
```

## Recuperação por cenário

**Kafka indisponível:** a API continua gravando. Verificar idade da outbox, recuperar Kafka e observar o relay publicar os pendentes. Não marcar eventos como publicados manualmente. Após um crash entre ack e commit SQL, o mesmo `event_id` pode ser publicado novamente.

**MySQL indisponível:** a API retorna `503`; o consumer não confirma registros não persistidos. Os processos tentam novamente. Se o consumer perder a participação no grupo e falhar ao confirmar, o Compose o reinicia. Investigar conexões e disponibilidade antes de aumentar réplicas.

**Risco indisponível ou lento:** timeout total de cinco segundos; erro temporário vira `RETRY_PENDING`. Recuperar a dependência e aguardar o próximo horário persistido. Reiniciar worker não reinicia contagem nem antecipa retries.

**Worker encerrado durante análise:** após 30 segundos, outro executor pode reclamar o job. Ele recebe um novo token; a conclusão antiga é rejeitada. Uma tentativa externa pode repetir, usando a mesma chave idempotente daquela geração.

**Mensagem inválida:** conferir `failure_id`, motivo e origem. O registro permanece no MySQL e seu aviso vai para DLQ. Corrigir o contrato e usar replay de quarentena; não apagar falhas nem redefinir offsets para contornar validação.

## Replay de falha técnica

Após corrigir a causa, inspecione e reenvie a transação:

```sh
docker compose exec api bamaq failures --limit 20
docker compose exec api bamaq replay UUID_DA_TRANSACAO
```

Somente `FAILED` é aceito. O comando retorna a nova geração em `PENDING`, grava eventos na outbox e marca o histórico da falha. O job anterior continua `DEAD`; o novo começa com zero tentativas. `APPROVED`, `REJECTED` ou uma transação já reenviada provocam erro de conflito e código de saída 2.

Replay de quarentena requer um envelope corrigido, com o formato de [architecture.md](architecture.md). Ele deve apontar para uma transação existente em `PENDING` e sua geração/versão atuais. Não altera dados financeiros.

```sh
docker compose cp corrected-event.json api:/tmp/corrected-event.json
docker compose exec api bamaq replay-quarantine UUID_DA_FALHA \
  --payload-file /tmp/corrected-event.json
```

Um novo `event_id` é criado e o histórico é preservado. Falha já reenviada ou geração que já possui job é rejeitada. O payload em quarentena pode conter dados sensíveis: inspecioná-lo diretamente no banco exige acesso operacional apropriado; a CLI imprime somente metadados.

## Simulações locais

O mock oferece modos controlados. Em PowerShell, por exemplo:

```powershell
$env:BAMAQ_MOCK_MODE = 'unavailable'
docker compose up -d --no-deps --force-recreate risk-mock
# Crie uma transação pela API e observe RETRY_PENDING.
$env:BAMAQ_MOCK_MODE = 'normal'
docker compose up -d --no-deps --force-recreate risk-mock
# Aguarde o horário persistido do próximo retry.
```

Para resposta lenta, use `BAMAQ_MOCK_DELAY_SECONDS=6`; para erro definitivo de contrato, use `BAMAQ_MOCK_MODE=invalid`. Para falhar somente as primeiras N chamadas por chave, use `BAMAQ_MOCK_FAIL_FIRST=N`. Em Bash, declare as mesmas variáveis com `export`.

Falha Kafka controlada: `docker compose stop kafka`, criar transação e consultar `PENDING`, depois `docker compose start kafka`. Outbox e offsets recuperam o fluxo. Crash do worker pode ser simulado com `docker compose kill -s SIGKILL worker` durante uma chamada lenta e `docker compose up -d worker`; a recuperação aguarda a expiração do lease.

Use a suíte automatizada para simular 30 minutos sem espera real. Ela também demonstra crash após commit MySQL e antes do commit Kafka.

## Migrações, retenção e limites

O serviço `migrate` roda `alembic upgrade head` antes dos processos dependentes. Em desenvolvimento local, o mesmo comando usa `BAMAQ_DATABASE_URL`. Mudanças futuras devem criar revisões novas e preservar compatibilidade durante rollout; não alterar a migração inicial de um ambiente já entregue.

O script de criação de `bamaq_test` é executado apenas em volumes MySQL novos. Para um volume anterior sem esse banco, um administrador local pode executar o conteúdo de `docker/mysql-init.sql` no banco antes dos testes; não é necessário descartar os dados existentes.

Kafka mantém eventos por sete dias no ambiente local. Jobs, outbox publicada e falhas permanecem no banco nesta versão. Antes de automatizar limpeza, definir a janela de auditoria/replay; manter deduplicação por pelo menos toda essa janela. Backups e recuperação do MySQL são essenciais porque trabalhos cujo offset já foi confirmado residem nele.

Uma réplica de relay mantém ordem e simplicidade. Escalar os executores com `docker compose up -d --scale worker=4`. O ganho deve ser medido em conjunto com conexões MySQL, latência do serviço de risco e backlog. Não aumentar partições ou publicar em paralelo sem rever a ordenação por transação.
