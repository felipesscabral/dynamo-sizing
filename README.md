# collect_dynamodb_sizing.sh

Script bash para coleta completa de métricas de sizing de tabelas **Amazon DynamoDB**, com foco em subsidiar processos de migração e dimensionamento para **MongoDB Atlas**.

Utiliza exclusivamente **AWS CLI v2** e **jq** — sem dependências adicionais.

---

## O que é coletado

### Por tabela

| Categoria | Dados |
|---|---|
| **Metadata** | Modo de cobrança, classe, status |
| **Chave primária** | Hash key + range key com tipos (S / N / B) |
| **Sizing** | Item count, tamanho total (bytes/MB), tamanho médio de item (bytes) |
| **Capacidade** | RCU/WCU provisionados, configuração de Auto Scaling |
| **TTL** | Habilitado/desabilitado, atributo configurado |
| **Streams** | Habilitado/desabilitado, view type |
| **PITR** | Habilitado/desabilitado |
| **Global Tables** | Regiões de replicação ativas |

### Por índice (GSI e LSI)

| Campo | Descrição |
|---|---|
| `index_type` | GSI ou LSI |
| `index_name` | Nome do índice |
| `hash_key` / `hash_key_type` | Chave de partição do índice e seu tipo |
| `range_key` / `range_key_type` | Chave de ordenação do índice e seu tipo |
| `projection_type` | ALL, KEYS_ONLY ou INCLUDE |
| `projected_attributes` | Atributos incluídos (quando INCLUDE) |
| `index_size_bytes` / `index_size_mb` | Tamanho do índice |
| `item_count` | Quantidade de itens no índice |
| `provisioned_rcu` / `provisioned_wcu` | Capacidade provisionada (GSI) |
| `cw_consumed_rcu_total` / `cw_consumed_wcu_total` | Consumo acumulado 60 dias (GSI) |
| `cw_throttled_reads_total` / `cw_throttled_writes_total` | Throttles 60 dias (GSI) |

### CloudWatch — janela de 60 dias

| Métrica | Estatística | Descrição |
|---|---|---|
| `ConsumedReadCapacityUnits` | Sum | Total e média diária de RCU consumidos |
| `ConsumedWriteCapacityUnits` | Sum | Total e média diária de WCU consumidos |
| `SuccessfulRequestLatency` | Average | Latência média de leitura e escrita (ms) |
| `ReadThrottleEvents` | Sum | Total de eventos de throttle de leitura |
| `WriteThrottleEvents` | Sum | Total de eventos de throttle de escrita |
| `SystemErrors` | Sum | Total de erros de sistema |
| `TransactionConflict` | Sum | Total de conflitos de transação |
| `ReturnedItemCount` | Average | Média de itens retornados por operação |

---

## Pré-requisitos

| Ferramenta | Versão mínima | Instalação |
|---|---|---|
| `aws cli` | v2 | [docs.aws.amazon.com/cli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) |
| `jq` | 1.6+ | `apt install jq` / `yum install jq` / `brew install jq` |

### Permissões IAM necessárias

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "dynamodb:DescribeTimeToLive",
        "dynamodb:DescribeContinuousBackups",
        "application-autoscaling:DescribeScalingPolicies",
        "cloudwatch:GetMetricStatistics"
      ],
      "Resource": "*"
    }
  ]
}
```

> Todas as ações são **somente leitura**. Nenhuma escrita ou modificação é realizada nas tabelas.

---

## Instalação

```bash
git clone <repo-url>
cd <repo>
chmod +x collect_dynamodb_sizing.sh
```

---

## Uso

```bash
# Configuração mínima
AWS_REGION=us-east-1 ./collect_dynamodb_sizing.sh

# Com região explícita
AWS_REGION=sa-east-1 ./collect_dynamodb_sizing.sh

# Com perfil AWS específico
AWS_PROFILE=minha-conta AWS_REGION=us-east-1 ./collect_dynamodb_sizing.sh
```

### Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `AWS_REGION` | `us-east-1` | Região AWS onde as tabelas estão |
| `AWS_PROFILE` | _(default)_ | Perfil do AWS CLI a utilizar |

---

## Arquivos de saída

Os arquivos são gerados em um diretório com timestamp: `dynamo_sizing_YYYYMMDD_HHMMSS/`

| Arquivo | Conteúdo |
|---|---|
| `<nome-tabela>.json` | Dados brutos completos por tabela em JSON estruturado |
| `sizing_report.json` | Consolidado de todas as tabelas em um único JSON |
| `sizing_summary.csv` | **Uma linha por tabela** — sizing, capacidade, índices e métricas CloudWatch |
| `indexes_detail.csv` | **Uma linha por índice (GSI/LSI)** — composição, projeção, tamanho e métricas |
| `cloudwatch_metrics.csv` | Série temporal diária de todas as métricas por tabela |

### Exemplo de `sizing_summary.csv`

```
table_name,billing_mode,table_class,status,item_count,table_size_bytes,table_size_mb,avg_item_size_bytes,...
Orders,PAY_PER_REQUEST,STANDARD,ACTIVE,1500000,524288000,500.0,349,...
Products,PROVISIONED,STANDARD,ACTIVE,80000,10485760,10.0,131,...
```

### Exemplo de `indexes_detail.csv`

```
table_name,index_type,index_name,hash_key,hash_key_type,range_key,range_key_type,projection_type,...
Orders,GSI,status-createdAt-index,status,S,createdAt,N,ALL,...
Orders,LSI,Orders-createdAt-index,customerId,S,createdAt,N,KEYS_ONLY,...
```

---

## Interpretação dos dados para sizing MongoDB Atlas

### Dimensionamento de cluster

| Métrica DynamoDB | Uso no sizing Atlas |
|---|---|
| `table_size_mb` (soma de todas as tabelas) | Storage mínimo do cluster |
| `avg_item_size_bytes` | Tamanho médio de documento MongoDB |
| `cw_consumed_rcu_avg_day` | Estimativa de operações de leitura/dia |
| `cw_consumed_wcu_avg_day` | Estimativa de operações de escrita/dia |
| `cw_read_latency_avg_ms` | Baseline de latência para SLA |

### Mapeamento de índices

| Conceito DynamoDB | Equivalente MongoDB |
|---|---|
| Hash key (PK simples) | `_id` ou campo indexado com índice único |
| Hash key + Range key | Índice composto `{ hashKey: 1, rangeKey: 1 }` |
| GSI | Índice secundário (`createIndex`) |
| LSI | Índice composto com o mesmo hash key da tabela |
| Projection `KEYS_ONLY` | Índice coberto (`covered index`) |
| Projection `INCLUDE` | Índice com campos específicos |
| TTL attribute | TTL index no Atlas (`expireAfterSeconds`) |

### Alertas de atenção

- **`cw_throttled_reads_total` ou `cw_throttled_writes_total` > 0** — workload está no limite da capacidade provisionada; considerar tier de cluster mais alto
- **`cw_transaction_conflicts` > 0** — uso de transações; garantir que o modelo MongoDB utilize sessões com `multi-document transactions`
- **`avg_item_size_bytes` > 50.000** — documentos grandes; avaliar padrão de compressão Zstandard no Atlas
- **`gsi_count` > 5 por tabela** — alta necessidade de acesso por padrões distintos; revisar modelo de dados para possível desnormalização no MongoDB
- **`projection_type = KEYS_ONLY`** — padrão de acesso em duas etapas (index lookup + item fetch); no MongoDB pode ser substituído por índices cobertos

---

## Troubleshooting

**`An error occurred (AccessDeniedException)`**
Verifique se o usuário/role possui todas as permissões listadas na seção de [Permissões IAM](#permissões-iam-necessárias).

**`command not found: jq`**
```bash
# Amazon Linux / RHEL
sudo yum install -y jq

# Ubuntu / Debian
sudo apt-get install -y jq

# macOS
brew install jq
```

**Tabela não aparece na listagem**
Confirme a região correta:
```bash
aws dynamodb list-tables --region $AWS_REGION --output table
```

**Métricas CloudWatch retornam zero**
Tabelas com baixo tráfego podem não ter datapoints em todos os períodos. Isso é esperado — o script registra `0` nesses casos.

---

## Notas

- O script é **somente leitura** e não realiza nenhuma alteração nas tabelas
- Tabelas com muitos GSIs podem levar mais tempo devido às chamadas CloudWatch por índice
- A janela de 60 dias pode ser ajustada alterando a variável `CW_DAYS` no início do script
- Em contas com centenas de tabelas, considere filtrar por prefixo de nome adaptando o trecho de `list-tables`
