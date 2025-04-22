
# `get_cost_data(session, time_range)`  
> Função principal para extrair custos da AWS via **Cost Explorer API** (não usa CUR)

---

###  Objetivo:
Extrair dados de:
- Gasto do período atual e anterior
- Gasto por serviço
- Orçamentos (Budgets)
- Intervalos customizados com `--time-range`

---

### Por que não usar CUR (Cost and Usage Report)?

Apesar do CUR ser mais detalhado (nível por recurso), ele exige:
- Configuração manual (S3, Athena, Glue)
- Armazenamento e manutenção de dados
- Transformações para leitura (CSV, Parquet, etc)

Este projeto **opta pela API do Cost Explorer** (`get_cost_and_usage`) por ser:
- Mais leve e rápida para dashboards
- Suficiente para visão FinOps consolidada
- Pronta para uso sem infra adicional

---

##  Análise por trecho

### 1. Sessões e datas

```python
ce = session.client("ce") # Cost Explorer
budgets = session.client("budgets", region_name="us-east-1")
today = date.today()
```

- Cria dois clientes: Cost Explorer e Budgets
- `budgets` é regional e só funciona em `us-east-1`
- `today` serve de base para calcular datas

---

### 2. Definição de intervalo de análise

```python
if time_range:
    start_date = today - timedelta(days=time_range)
    end_date = today
    previous_period_start = start_date - timedelta(days=time_range)
    previous_period_end = start_date - timedelta(days=1)
else:
    start_date = today.replace(day=1)
    end_date = today
    previous_period_end = start_date - timedelta(days=1)
    previous_period_start = previous_period_end.replace(day=1)
```

- `time_range`: número de dias para análise (ex: últimos 30 dias)
- Se omitido, assume mês atual
- Define também o período anterior para comparação

---

### 3. Consulta ao Cost Explorer

#### Período atual (total)
```python
ce.get_cost_and_usage(
    TimePeriod={"Start": ..., "End": ...},
    Granularity="MONTHLY",
    Metrics=["UnblendedCost"]
)
```

- Traz o custo consolidado no período atual
- `UnblendedCost`: custo real (sem rateio proporcional de savings plans)

#### Período anterior (total)
Mesma estrutura, com datas anteriores.

---

### 4. Custo por serviço

```python
ce.get_cost_and_usage(
    TimePeriod={"Start": ..., "End": ...},
    Granularity="DAILY" if time_range else "MONTHLY",
    Metrics=["UnblendedCost"],
    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}]
)
```

- Aqui está o coração da análise de **custo por serviço**
- Usa `GroupBy SERVICE` para obter EC2, S3, RDS, etc
- Se `time_range` foi informado, usa `DAILY` para granularidade mais fina

---

### 5. Agregação de custos por serviço

```python
aggregated_service_costs = defaultdict(float)
for result in current_period_cost_by_service.get("ResultsByTime", []):
    for group in result.get("Groups", []):
        service = group["Keys"][0]
        amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
        aggregated_service_costs[service] += amount
```

- Soma os valores de cada serviço ao longo dos dias
- Evita múltiplas entradas para o mesmo serviço

---

### 6. Orçamentos (Budgets)

```python
budgets.describe_budgets(AccountId=account_id)
```

- Busca todos os orçamentos da conta atual
- Extrai nome, limite, valor atual e previsão (`forecast`)

---

### 7. Retorno final

```python
return {
    "account_id": account_id,
    "current_month": current_period_cost,
    "last_month": previous_period_cost,
    "current_month_cost_by_service": aggregated_groups,
    "budgets": budgets_data,
    ...
}
```

- Dados preparados para uso em dashboards ou exportação
- Inclui nomes dos períodos para visualização legível

---

## `process_service_costs`

```python
cost_amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
if cost_amount > 0.001:
    service_cost_data.append((service_name, cost_amount))
```

- Filtra serviços com custo irrelevante (< R$ 0,01)
- Ordena do maior para o menor (prioridade de análise)

---

## Exportações: `export_to_csv` / `export_to_json`

