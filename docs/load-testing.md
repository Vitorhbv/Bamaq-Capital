# Teste de carga local

O teste executa requisições reais contra a API e acompanha o processamento persistido no MySQL. No runner Docker também consulta offsets Kafka, sem consumir mensagens, ingressar no grupo de processamento ou confirmar offsets. Usa apenas dependências já fixadas no projeto.

## Execução recomendada pelo Docker

Execute na raiz do repositório, com o mock em modo `normal`, limite de aprovação padrão de BRL 5.000,00 e sem falhas artificiais:

```sh
docker compose up -d --build --wait
docker compose --profile load run --rm load-test
```

A execução padrão oferece `5:10,10:10`: 5 transações/segundo por 10 segundos e 10/segundo por outros 10 segundos, totalizando 150 transações. Depois da última requisição, aguarda até 300 segundos para concluir o processamento e publicar os eventos pendentes.

No Linux, crie o diretório de relatórios como seu usuário e execute o runner com seu UID/GID para que o volume montado seja gravável:

```sh
mkdir -p reports/load-tests
docker compose --profile load run --rm --user "$(id -u):$(id -g)" load-test
```

O runner verifica a permissão de gravação antes de gerar tráfego. No Docker Desktop/Windows, o comando padrão já grava nesse diretório.

Estágios customizados seguem `RPS:SEGUNDOS`, separados por vírgula:

```sh
docker compose --profile load run --rm load-test --stages "10:15,25:30,50:30"
```

O preset do case é explícito:

```sh
docker compose --profile load run --rm load-test --case
```

Ele oferece `25:30,50:30,100:30,167:120`, criando até **25.290 transações** em 210 segundos, mais o tempo para concluir a fila. O gerador limita requisições simultâneas a 64 e não acumula uma fila ilimitada em memória. Slots que não puderem ser enviados são contados como descartados e tornam o resultado reprovado; assim, reduzir silenciosamente a carga não gera uma aprovação falsa.

## Transações e eventos

As taxas do gerador medem **transações recebidas por segundo**. Uma transação normal gera quatro eventos: comando de processamento, status `PENDING`, `PROCESSING` e `APPROVED/REJECTED`. Retries produzem eventos adicionais.

- 167 transações/segundo correspondem a aproximadamente 10.000 transações de entrada/minuto e 40.000 eventos Kafka/minuto.
- Para aproximadamente 10.000 eventos Kafka totais/minuto sem retries, use cerca de 42 transações/segundo: `--stages "42:120"`.

O teste alterna valores de BRL 1.500,00 e BRL 6.000,00 para exercitar aprovação e rejeição. Valores e resultados são verificados; mudanças intencionais na configuração do mock podem reprovar essa expectativa.

## Limites de aceitação e comparação de workers

Sem limites opcionais, a aprovação verifica envio, respostas, persistência, unicidade, resultados finais e drenagem da outbox. Ela não significa automaticamente que o sistema sustenta a taxa com uma latência aceitável.

Exemplo com critérios explícitos, que podem ser ajustados ao objetivo da medição:

```sh
docker compose --profile load run --rm load-test --case \
  --max-api-p95-ms 1000 --max-e2e-p95-ms 10000 --max-backlog 1000
```

No PowerShell, mantenha o comando em uma linha ou substitua as barras de continuação por crases. O backlog é a quantidade de transações ainda não terminais observada em cada amostra; o limite se aplica ao pico observado, não somente ao valor final.

Para comparar escala, execute a mesma carga com uma e depois quatro réplicas. Aguarde a conclusão da primeira execução antes da seguinte:

```sh
docker compose up -d --scale worker=1 --wait
docker compose --profile load run --rm load-test --stages "10:30,25:60"
docker compose up -d --scale worker=4 --wait
docker compose --profile load run --rm load-test --stages "10:30,25:60"
```

Não há promessa de alcançar a taxa: CPU, memória, disco, Docker, MySQL, o relay único e a latência do risco influenciam os resultados. Se a fila continuar crescendo durante a carga e só cair após o envio parar, a taxa de entrada supera a capacidade de processamento naquele cenário.

## Relatórios

Cada execução gera uma pasta `reports/load-tests/load-UUID/`, ignorada pelo Git:

| Arquivo | Conteúdo |
|---|---|
| `report.json` | Configuração sem credenciais do banco, resultado, verificações, estágios e amostras |
| `requests.csv` | Sequência, estágio, HTTP, latência, ID retornado e classe do erro |
| `transactions.csv` | Transações persistidas, resultado esperado/obtido e tempo até estado final |
| `samples.csv` | Evolução dos estados, jobs, retries, outbox e atraso Kafka |

As latências usam percentis por posição mais próxima (`nearest rank`). A latência da API inclui toda a requisição HTTP. O tempo de processamento usa `updated_at - created_at` no estado terminal e inclui espera por publicação, consumo, job, análise e retries; não inclui tempo de conexão HTTP anterior à persistência. Ele depende de relógios sincronizados entre API e workers.

Medições SQL são filtradas pelo prefixo único de idempotência da execução. O lag Kafka é de todo o grupo `bamaq-job-ingestion-v1` e pode incluir outro tráfego. As consultas de monitoramento e o gerador também consomem recursos da máquina. Picos entre amostras podem não ser capturados.

Uma resposta `202` não basta: o relatório verifica se os IDs aceitos correspondem aos persistidos e se as transações chegam ao resultado esperado. Timeouts HTTP podem deixar transações já gravadas; elas aparecem na reconciliação e a execução reprova, em vez de concluir que não foram criadas.

Códigos de saída: `0` verificações aprovadas; `1` carga/resultado/limites reprovados, com relatório; `2` erro de configuração, dependências ou gravação de relatório; `130` interrupção pelo usuário. Uma interrupção não desfaz transações já criadas. O limite padrão é 100.000 requisições planejadas; `--max-requests` permite alterá-lo conscientemente.

## Execução pelo Python do host

Com o ambiente virtual do README e os serviços Docker iniciados:

```powershell
.\.venv\Scripts\python.exe scripts/load_test.py --stages "5:10,10:10" --kafka-bootstrap-servers localhost:9092
```

O monitor usa `BAMAQ_LOAD_DATABASE_URL`, `--database-url` ou a configuração MySQL do projeto (`BAMAQ_DATABASE_URL`/`.env`, por padrão localhost:3306). A URL da API pode ser alterada por `--base-url`. `--help` lista opções de concorrência, timeout, intervalo de amostragem e diretório de saída.

As transações usam `customer_id=load-UUID` e `Idempotency-Key=load-UUID:SEQUENCIA`, e permanecem no banco `bamaq` para inspeção pelo Adminer. Não existe limpeza automática, e o banco `bamaq_test` não é usado pelo benchmark.

Um resultado local deve ser apresentado junto da CPU/memória disponibilizadas ao Docker, número de workers, configuração do mock, duração, taxa efetivamente enviada, percentis e evolução do backlog. O relatório registra Python, plataforma e CPUs lógicas do gerador; recursos alocados ao Docker devem ser anotados separadamente.
