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

Não foi executado benchmark de 10.000 eventos/minuto. A indisponibilidade de 30 minutos é simulada com relógio controlável. O workflow CI foi preparado e seus comandos essenciais foram executados localmente; o workflow hospedado só será executado após publicação no GitHub.
