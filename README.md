# 🚀 Projeto de Engenharia de Dados: Ingestão Automatizada de Indicadores Públicos (IBGE e BCB)

## 📌 Visão Geral
Este projeto visa automatizar a coleta, transformação e carga de dados públicos dos principais indicadores econômicos e sociais fornecidos por **IBGE** e **Banco Central do Brasil (BCB)**. Os dados são estruturados e armazenados no **Google BigQuery**, prontos para análises e visualizações avançadas.

A solução foi desenvolvida com foco em **modularidade, boas práticas de engenharia de dados** e integração com o **Google Cloud Platform**, utilizando **Python**, **Airflow** e **Cloud Composer**.

## 🎯 Objetivos
- Automatizar a ingestão de dados via APIs públicas (IBGE e BCB).
- Transformar dados brutos em formato analítico utilizando `pandas`.
- Armazenar os dados em tabelas particionadas e otimizadas no BigQuery.
- Agendar execuções com o Airflow via Cloud Composer.
- Garantir **idempotência**, **reusabilidade** e **observabilidade** dos pipelines.

## ⚙️ Tecnologias e Bibliotecas
- Python 3.10+
- Apache Airflow (via Cloud Composer)
- Google BigQuery
- Google Cloud Platform (GCP)
- `requests`, `pandas`
- `google-cloud-bigquery`
- `apache-airflow-providers-google`
- `python-dotenv` (execução local)

## 📁 Estrutura do Projeto
```plaintext
ingestao_dados_publicos/
├── config/
│   └── gcp_credentials.json         # Chave da Service Account
├── dags/
│   ├── ibge_pipeline_dag.py         # DAG Airflow - IBGE
│   ├── bcb_pipeline_dag.py          # DAG Airflow - BCB
├── src/                         # Código-fonte dos pipelines
│   ├── common/
│   │   ├── bigquery_operations.py
│   │   └── utils.py
│   ├── ibge_pipeline/
│   │   ├── extractor.py
│   │   ├── transformer.py
│   │   └── main_ibge.py
│   └── bcb_pipeline/
│       ├── extractor.py
│       ├── transformer.py
│       └── main_bcb.py
├── notebooks/                       # Análises exploratórias (opcional)
├── tests/                           # Testes unitários
├── .env                             # Variáveis de ambiente local
├── requirements.txt
└── README.md
```

## 🛠️ Configuração do Ambiente Local
1. **Clonar o repositório**
   ```bash
   git clone https://github.com/HygorSX/proj-ingestao-dados.git
   cd ingestao_dados_publicos
   ```

2. **Criar ambiente virtual e instalar dependências**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Criar arquivo .env**
   ```ini
   # .env
   GCP_PROJECT_ID=seu-projeto-gcp
   BIGQUERY_DATASET_IBGE=dados_publicos_ibge
   BIGQUERY_DATASET_BCB=dados_publicos_bcb
   GCP_LOCATION=southamerica-east1
   GOOGLE_APPLICATION_CREDENTIALS=./config/gcp_credentials.json
   ```

## ☁️ Configuração no Google Cloud
1. **Criar Projeto GCP e ativar APIs**
   - BigQuery API
   - Cloud Composer API

2. **Criar Service Account**
   - Permissões: BigQuery Data Editor, BigQuery Job User, Storage Object Admin, Composer Worker

3. **Criar ambiente no Cloud Composer**
   - Versão recomendada: Composer 2.x / Airflow 2.x
   - Região: southamerica-east1

   Suba a pasta `dags/` e `src/` para o bucket:
   ```bash
   gsutil -m cp -r dags/ gs://<seu-bucket-composer>/dags/
   ```

4. **Configurar Variáveis na UI do Airflow (Admin > Variables)**
   | Key                  | Value                      |
   |----------------------|----------------------------|
   | GCP_PROJECT_ID       | ingestao-dados-publicos    |
   | BIGQUERY_DATASET_IBGE| dados_publicos_ibge        |
   | BIGQUERY_DATASET_BCB | dados_publicos_bcb         |
   | GCP_LOCATION         | southamerica-east1         |

## 🔄 Pipelines Desenvolvidos
### 📊 IBGE
Indicadores tratados:
- IPCA - Variação Mensal
- Taxa de Desocupação
- PIB Anual
- População Estimada

### 💰 BCB
Séries econômicas:
- SELIC (várias variantes)
- Dólar e Euro (PTAX)
- Inadimplência de crédito
- Saldo de crédito para PF

**Etapas do ETL**
- **Extração**: APIs públicas (`requests`)
- **Transformação**: `pandas`, tratamento de datas, valores nulos, tipos
- **Carga**: Tabela staging + MERGE → Tabela final no BigQuery

## ⏱️ Agendamento e Execução com Airflow
As DAGs foram criadas com:
- Agendamento diário (`@daily`)
- Logging centralizado com `setup_logging`
- Funções Python modulares
- Retry com delay padrão

```python
with DAG(... schedule_interval='@daily', catchup=False):
    PythonOperator(
        task_id='run_ibge_pipeline',
        python_callable=run_all_ibge_pipelines
    )
```

## ♻️ Idempotência com BigQuery
Para evitar duplicações, o projeto usa:
- Tabelas de staging
- Comando MERGE para consolidar dados
- Remoção da tabela temporária após uso

**Benefícios**:
- Pode ser reexecutado com segurança
- Sem sobrescrever históricos
- Atualizações incrementais possíveis

## ▶️ Execução Manual
**Local**
```bash
python src/ibge_pipeline/main_ibge.py
python src/bcb_pipeline/main_bcb.py
```

**No Airflow**
- Vá na UI do Airflow
- Clique em “Trigger DAG” ▶️
- Acompanhe os logs da tarefa

## 🧪 Consultas SQL de Validação
**IPCA (IBGE)**
```sql
SELECT
  EXTRACT(YEAR FROM data_referencia) AS ano,
  EXTRACT(MONTH FROM data_referencia) AS mes,
  COUNT(*) AS registros,
  AVG(valor_serie) AS media_ipca
FROM `ingestao-dados-publicos.dados_publicos_ibge.ibge_ipca_variacao_mensal_brasil`
GROUP BY ano, mes
ORDER BY ano DESC, mes DESC
LIMIT 10;
```

**SELIC (BCB)**
```sql
SELECT
  data_referencia,
  valor_serie,
  codigo_serie
FROM `ingestao-dados-publicos.dados_publicos_bcb.bcb_selic_diaria`
ORDER BY data_referencia DESC
LIMIT 10;
```

## ✅ Boas Práticas Implementadas
- Estrutura modular por pipeline e função (ETL separado)
- Logging em todos os estágios
- Tratamento de exceções e alertas
- Uso de `.env` local e `Variable.get()` no Airflow
- Versionamento com `.gitignore` e organização do projeto
- Idempotência com BigQuery (MERGE, staging)

## 📌 Observações Finais
**Oportunidades de evolução**:
- Paralelização por indicador no DAG (via TaskGroup)
- Notificações por Slack ou email em falhas
- Integração com Looker Studio para dashboards
- Monitoramento com Grafana + Prometheus (ou Cloud Logging)

Engenharia de Dados Moderna não é só sobre mover dados, mas orquestrar confiabilidade, escalabilidade e rastreabilidade.