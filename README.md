# Pipeline de Processamento de Dados com BigQuery e Cloud Storage

## Visão Geral
Este projeto implementa um pipeline de processamento de dados utilizando o **Google Cloud Storage (GCS)** e **BigQuery**. O objetivo principal é processar arquivos **Excel** armazenados em um bucket no **GCS**, transformá-los em um formato estruturado, salvando em **Parquet** em uma nova pasta no **GCS**, depois carregá-los em um **Star Schema** no **BigQuery**.

## Estrutura do Projeto

```
.
├── configs/
│   ├── config.yaml
├── hooks/
│   ├── bigquery_hook.py
│   ├── storage_hook.py
├── modules/
│   ├── bigquery_base.py
│   ├── excel_processor.py
│   ├── star_schema.py
│   ├── storage_base.py
├── pipeline/
│   ├── pipeline.py
├── main.py
├── requirements.txt
```

### Componentes Principais
- **config.yaml**: Contém as configurações do projeto, como **ID do projeto**, **nome do bucket** e **dataset do BigQuery**.
- **bigquery_hook.py**: Responsável pela conexão com o **BigQuery**.
- **storage_hook.py**: Responsável pela conexão com o **Google Cloud Storage**.
- **bigquery_base.py**: Realiza as principais interações com o **BigQuery**: cria, trunca, dropa e insere numa tabela, além de realizar a execução de queries.
- **storage_base.py**: Realiza as principais interações com o **Google Cloud Storage**: lista, lê, salva e deleta arquivos de um bucket.
- **excel_processor.py**: Lê e transforma os arquivos **Excel**, preparando-os para carregamento.
- **star_schema.py**: Constrói o **Star Schema** no **BigQuery**.
- **pipeline.py**: Orquestra o fluxo do pipeline, chamando os componentes necessários.
- **main.py**: Script principal para execução do pipeline.
- **requirements.txt**: Lista das bibliotecas necessárias para rodar o projeto.

## Configuração e Execução
### 1. Configuração do Ambiente
Antes de executar o pipeline, clone o repositório na máquina local e instale as dependências necessárias.

```bash
pip install -r requirements.txt
```

### 2. Configurando o `config.yaml`
Será necessário preencher as 3 variáveis presentes no arquivo na pasta `/configs` com os dados passados no enunciado da atividade.

```python
project_id: ""
bucket_name: ""
dataset_id: ""
```

### 3. Configuração das Credenciais
O projeto requer autenticação no **Google Cloud**. Certifique-se de que o arquivo de credenciais `vena-processo-seletivo-9ff459764064.json` esteja no diretório correto e defina a variável de ambiente no novo arquivo `.env` que será criado dentro da pasta `src`:

```python
GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/vena-processo-seletivo-9ff459764064.json
```

### 4. Execução do Pipeline
Para executar o pipeline, rode o seguinte comando:

```bash
python main.py
```

### 4. Fluxo do Pipeline
1. **Busca arquivos Excel no GCS (pasta `rawdata/`)**.
2. **Processa e transforma os dados** para um formato estruturado.
3. **Salva os arquivos processados em formato Parquet** no GCS (pasta `processed/`).
4. **Carrega os dados no BigQuery**, organizando-os em um **Star Schema**.

## Estrutura do Star Schema
O modelo de dados segue um formato **estrela**, garantindo eficiência nas consultas analíticas:

- **Dimensões:**
  - `Dim Conta`: Identifica as contas financeiras.
  - `Dim Unidade`: Representa as unidades comerciais.
  - `Dim Calendário`: Organiza as informações temporais.
  - `Dim Tipo`: Classifica os tipos de contas.

- **Tabela Fato:**
  - Contém os IDs das dimensões, a data de referência e o valor consolidado.

## Considerações Finais
Este pipeline é altamente escalável e pode ser adaptado para diferentes fontes de dados. Certifique-se de que os recursos do **Google Cloud** estão devidamente configurados para garantir o funcionamento correto.

