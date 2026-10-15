# Validação da implementação

Verificações executadas em 13/10/2026, com Docker Desktop/Compose operacionais.

| Verificação | Resultado |
|---|---|
| Suíte no host Windows, Python 3.13.14 | 63 testes passaram |
| Suíte na imagem Linux, Python 3.13.3 | 63 testes passaram |
| MySQL real | 8.4.5; migrações, rollback, unicidade, locks e concorrência validados |
| Kafka real | 4.1.2; publicação, redelivery sem commit, recuperação e DLQ validadas |
| Cobertura medida no host | 84% das instruções de `src/bamaq` |
| Ruff lint e format | Aprovados |
| `pip check` | Dependências consistentes |
| `docker compose up -d --no-build --wait` | Todos os serviços contínuos saudáveis |
| `alembic check` | Nenhuma operação de migração pendente |
| SVG | XML válido e renderização visual conferida em navegador |

O smoke test realizou POST, publicação por outbox, ingestão Kafka, execução HTTP e consulta final para dois casos: BRL 1.500,00 aprovado e BRL 6.000,00 rejeitado.

Uma verificação operacional adicional iniciou o mock com contrato inválido, criou uma transação, observou `FAILED`, restaurou o mock e executou `bamaq replay` no container. A geração 2 concluiu como `APPROVED`, preservando a falha da geração anterior.

O teste de concorrência real identificou uma consulta inicial com OR/ordenação que bloqueava candidatos adicionais. A implementação final utiliza duas consultas indexadas e o teste confirmou que outro executor consegue selecionar um trabalho enquanto o primeiro permanece bloqueado.

A suíte emite um aviso de depreciação de `BlockingPortal` na combinação fixada de Starlette/AnyIO; não há falha associada. A cobertura inclui adaptadores e CLI, mas não representa cobertura completa do loop operacional; os processos foram exercitados pelo smoke test no Compose.

Não foi executado benchmark sustentado de 10.000 eventos/minuto nessa validação inicial. A indisponibilidade de 30 minutos é simulada com relógio controlável. Em 15/10/2026, o CI hospedado da `main` também passou após a correção da prontidão TCP do MySQL; esse resultado antecede a inclusão do teste de carga descrito abaixo.

## Teste de carga local — 15/10/2026

O gerador `scripts/load_test.py` e o perfil Compose `load` foram adicionados. A suíte passou com 83 testes no host Windows e na imagem Linux, incluindo testes de carga simulada, monitor MySQL real e leitura de offsets Kafka. O cenário de carga não é executado automaticamente no CI; os testes automatizados do gerador fazem parte da suíte.

Uma execução real com `--sample-interval 1 --drain-timeout 60` ofereceu 5 transações/segundo por 10 segundos e 10/segundo por outros 10 segundos:

| Medição | Resultado |
|---|---|
| Transações planejadas/enviadas/aceitas/persistidas/concluídas | 150 em cada contagem |
| Aprovações / rejeições / falhas técnicas | 75 / 75 / 0 |
| Eventos publicados | 600 |
| Outbox pendente e lag Kafka ao final | 0 / 0 |
| Latência HTTP p95 | 11,233 ms |
| Tempo até resultado final p95 | 898,931 ms |
| Pico observado de transações pendentes | 8 |
| Resultado das verificações | Aprovado |

Relatórios locais: `reports/load-tests/load-0676a5f1-1fe6-44cb-bf22-ecd679342679/`. Essa execução verificou funcionamento do gerador, relatório e fluxo completo; a suíte automatizada também estava em execução na máquina, portanto esses números não são um benchmark isolado de capacidade.

O preset `--case` está disponível para uma medição progressiva até 167 transações de entrada/segundo por dois minutos. Consultar `docs/load-testing.md` para configurar limites, duração e comparar workers. Disponibilizar o preset não comprova que essa taxa é sustentada pelo sistema.

Uma segunda verificação real ofereceu 167 transações/segundo por apenas três segundos (`--stages "167:3"`), com um worker. Todas as 501 transações foram aceitas e concluídas, e os 2.004 eventos foram publicados. Houve pico observado de 459 transações pendentes e 953 eventos na outbox; o tempo até resultado final p95 foi de 12.356,632 ms, e a execução terminou em 23,375 segundos após drenar a outbox. Relatórios: `reports/load-tests/load-71c46155-fcfa-4488-be73-54ef0a5c7e93/`.

Essa rajada valida o gerador na taxa configurada, mas o acúmulo de fila mostra que a configuração com um worker não acompanhou essa entrada durante o intervalo. Ela não comprova processamento sustentado de 10.000 transações/minuto. Os resultados JSON distinguem as verificações de conclusão dos limites opcionais de latência e backlog.
