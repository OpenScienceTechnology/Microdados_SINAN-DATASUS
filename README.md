# 🩺 DATASUS SINAN-VIOL Pipeline

> **Pipeline automatizado de download, conversão e compressão dos microdados de Violência Doméstica, Sexual e/ou Outras Violências do SINAN/DATASUS (2009–2025)**

---

## 📋 Sumário

- [Visão Geral](#-visão-geral)
- [Fonte dos Dados](#-fonte-dos-dados)
- [Estrutura de Arquivos](#-estrutura-de-arquivos)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Como Usar](#-como-usar)
- [Saídas Geradas](#-saídas-geradas)
- [Relatório de Execução](#-relatório-de-execução)
- [Uso dos Dados em Análise e Machine Learning](#-uso-dos-dados-em-análise-e-machine-learning)
- [Cobertura Temporal](#-cobertura-temporal)
- [Notas Técnicas](#-notas-técnicas)
- [Licença](#-licença)

---

## 🔍 Visão Geral

Este projeto automatiza a coleta e o pré-processamento dos microdados públicos de **violência doméstica, sexual e/ou outras violências** do Sistema de Informação de Agravos de Notificação (**SINAN**), disponibilizados pelo **DATASUS/Ministério da Saúde do Brasil**.

O pipeline executa três etapas encadeadas:

```
FTP DATASUS
    │
    ▼
[1] Download .dbc ──► arquivo original comprimido (formato proprietário DATASUS)
    │
    ▼
[2] Descompressão .dbc → .dbf ──► formato dBASE intermediário (temporário)
    │
    ▼
[3] Conversão .dbf → .csv + .csv.gz ──► prontos para análise e ML
```

**Todos os arquivos ficam na mesma pasta do script**, sem subdiretórios.

---

## 📦 Fonte dos Dados

| Item | Detalhe |
|---|---|
| **Sistema** | SINAN — Sistema de Informação de Agravos de Notificação |
| **Agravo** | Violência Doméstica, Sexual e/ou Outras Violências (`VIOL`) |
| **Servidor FTP** | `ftp.datasus.gov.br` |
| **Caminho (finais)** | `/dissemin/publicos/SINAN/DADOS/FINAIS/` |
| **Caminho (prelim)** | `/dissemin/publicos/SINAN/DADOS/PRELIM/` |
| **Referência** | [alpaca.quantilica.com/dados/sinan-viol](https://alpaca.quantilica.com/dados/sinan-viol) |
| **Gestor** | Ministério da Saúde — SVSA/DATASUS |

> Os dados são públicos e de livre uso, conforme a Lei de Acesso à Informação (Lei nº 12.527/2011).

---

## 🗂️ Estrutura de Arquivos

Após a execução completa, a pasta conterá:

```
📁 /pasta_do_script/
│
├── 📄 datasus_viol_pipeline.py          ← script principal
│
├── 🗃️  VIOLBR09.dbc                     ─┐
├── 📊  VIOLBR09.csv                      │
├── 🗜️  VIOLBR09.csv.gz                   │  um conjunto por ano
│                                          │  (2009 → 2025)
├── 🗃️  VIOLBR25.dbc                     ─┘
├── 📊  VIOLBR25.csv
├── 🗜️  VIOLBR25.csv.gz
│
└── 📋  relatorio_execucao_YYYYMMDD_HHMMSS.txt
```

| Extensão | Descrição | Uso |
|---|---|---|
| `.dbc` | Original DATASUS — comprimido proprietário | Preservação / reprocessamento |
| `.csv` | Tabela plana, `utf-8-sig` | Excel, LibreOffice, pandas |
| `.csv.gz` | CSV comprimido com gzip | pandas, TensorFlow, PyTorch, Spark |
| `.txt` | Relatório de execução | Auditoria, rastreabilidade |

---

## ⚙️ Pré-requisitos

- Python **3.9+**
- Acesso à internet (FTP DATASUS)

### Dependências Python

```bash
pip install dbfread pandas datasus-dbc
```

| Pacote | Versão mínima | Finalidade |
|---|---|---|
| `pandas` | 1.5+ | Leitura DBF e geração CSV |
| `dbfread` | 2.0+ | Parser do formato dBASE (.dbf) |
| `datasus-dbc` | 0.1+ | Descompressão do formato .dbc proprietário |

> `gzip`, `shutil`, `urllib`, `os`, `time`, `datetime` são módulos da biblioteca padrão do Python — sem instalação adicional.

---

## 🚀 Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/datasus-viol-pipeline.git
cd datasus-viol-pipeline

# 2. (Opcional) Crie um ambiente virtual
python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

# 3. Instale as dependências
pip install -r requirements.txt
```

### `requirements.txt`

```
dbfread>=2.0.7
pandas>=1.5.0
datasus-dbc>=0.1.0
```

---

## ▶️ Como Usar

```bash
python datasus_viol_pipeline.py
```

O script é **idempotente**: se `.csv` e `.csv.gz` de um arquivo já existirem, ele é pulado automaticamente — seguro para re-execuções parciais.

### Saída esperada no terminal

```
────────────────────────────────────────────────────────────
  [01/17]  VIOLBR09.dbc
────────────────────────────────────────────────────────────
  ↓ Download (tentativa 1/3): ftp://ftp.datasus.gov.br/...
  ✔ Download OK  [2.3 MB]
  ⚙  Descomprimindo DBC → DBF…
  ✔ DBF gerado  [8.1 MB]
  📖 Lendo DBF…
  ✔ 47.832 registros × 94 colunas
  💾 Salvando CSV…
  ✔ CSV salvo  [12.4 MB]
  🗜  Comprimindo → CSV.GZ…
  ✔ CSV.GZ salvo [1.8 MB]  (redução: 85.5%)
  🗑  DBF temporário removido.
  ⏱  Tempo do arquivo: 34.2s
```

---

## 📤 Saídas Geradas

### Arquivos de dados

Cada ano de competência gera **três arquivos**:

```python
VIOLBR{AA}.dbc      # ex: VIOLBR23.dbc  — original preservado
VIOLBR{AA}.csv      # ex: VIOLBR23.csv  — tabela plana
VIOLBR{AA}.csv.gz   # ex: VIOLBR23.csv.gz — comprimido (~80–90% menor)
```

### Relatório de execução (`relatorio_execucao_*.txt`)

Gerado automaticamente com:

- Status de cada arquivo (`OK`, `PULADO`, `ERRO_DOWNLOAD`, `ERRO_DECOMPRESS`, `ERRO_CONVERSAO`)
- Número de registros e colunas
- Tamanho dos arquivos (`.dbc`, `.csv`, `.csv.gz`)
- Taxa de compressão atingida
- Tempo de processamento por arquivo e total
- Demonstração de leitura dos arquivos gerados

---

## 📊 Uso dos Dados em Análise e Machine Learning

### Leitura básica com pandas

```python
import pandas as pd

# CSV plano
df = pd.read_csv("VIOLBR23.csv", encoding="utf-8-sig")

# CSV comprimido (recomendado para economizar disco/memória)
df = pd.read_csv("VIOLBR23.csv.gz", compression="gzip", encoding="utf-8-sig")

print(df.shape)        # (n_registros, n_colunas)
print(df.head())
```

### Concatenar todos os anos

```python
import glob

arquivos = sorted(glob.glob("VIOLBR*.csv.gz"))

df_total = pd.concat(
    [pd.read_csv(f, compression="gzip", encoding="utf-8-sig") for f in arquivos],
    ignore_index=True
)

print(f"Total de registros: {len(df_total):,}")
```

### Leitura em chunks (datasets grandes)

```python
for chunk in pd.read_csv(
    "VIOLBR23.csv.gz",
    compression="gzip",
    encoding="utf-8-sig",
    chunksize=100_000
):
    # processamento incremental
    pass
```

### Scikit-learn

```python
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv("VIOLBR23.csv.gz", compression="gzip", encoding="utf-8-sig")
df.dropna(axis=1, how="all", inplace=True)

for col in df.select_dtypes(include="object").columns:
    df[col] = LabelEncoder().fit_transform(df[col].astype(str))

X = df.drop(columns=["TP_VIOL"])   # ajuste para sua coluna alvo
y = df["TP_VIOL"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
```

### TensorFlow / Keras

```python
import tensorflow as tf

dataset = tf.data.experimental.make_csv_dataset(
    "VIOLBR23.csv.gz",
    batch_size=256,
    compression_type="GZIP"
)
```

### PyTorch

```python
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

class ViolDataset(Dataset):
    def __init__(self, path):
        self.df = pd.read_csv(path, compression="gzip", encoding="utf-8-sig")

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx].values
        return torch.tensor(row[:-1], dtype=torch.float32), \
               torch.tensor(row[-1],  dtype=torch.long)

loader = DataLoader(ViolDataset("VIOLBR23.csv.gz"), batch_size=256, shuffle=True)
```

---

## 📅 Cobertura Temporal

| Arquivo | Ano | Tipo | Registros (aprox.) |
|---|---|---|---|
| `VIOLBR09.dbc` | 2009 | Final | ~50 mil |
| `VIOLBR10.dbc` | 2010 | Final | ~80 mil |
| `VIOLBR11.dbc` | 2011 | Final | ~120 mil |
| `VIOLBR12.dbc` | 2012 | Final | ~170 mil |
| `VIOLBR13.dbc` | 2013 | Final | ~190 mil |
| `VIOLBR14.dbc` | 2014 | Final | ~210 mil |
| `VIOLBR15.dbc` | 2015 | Final | ~220 mil |
| `VIOLBR16.dbc` | 2016 | Final | ~230 mil |
| `VIOLBR17.dbc` | 2017 | Final | ~240 mil |
| `VIOLBR18.dbc` | 2018 | Final | ~250 mil |
| `VIOLBR19.dbc` | 2019 | Final | ~260 mil |
| `VIOLBR20.dbc` | 2020 | Final | ~220 mil |
| `VIOLBR21.dbc` | 2021 | Final | ~250 mil |
| `VIOLBR22.dbc` | 2022 | Final | ~280 mil |
| `VIOLBR23.dbc` | 2023 | Final | ~290 mil |
| `VIOLBR24.dbc` | 2024 | Final | ~290 mil |
| `VIOLBR25.dbc` | 2025 | **Preliminar** | em aberto |

> ⚠️ O arquivo de 2025 é **preliminar** (`PRELIM`) e pode ser atualizado ao longo do ano pelo DATASUS.

---

## 🔧 Notas Técnicas

### Formato `.dbc`
O formato `.dbc` é um **dBASE comprimido proprietário** utilizado exclusivamente pelo DATASUS. Não é compatível com leitores DBF comuns — requer a biblioteca [`datasus-dbc`](https://pypi.org/project/datasus-dbc/) para descompressão.

### Encoding
Os arquivos DBF do DATASUS utilizam codificação **ISO-8859-1** (Latin-1). Os CSVs gerados são salvos em **UTF-8 com BOM** (`utf-8-sig`) para garantir compatibilidade com Excel, LibreOffice e ferramentas de análise.

### Arquivo `.dbf` temporário
O `.dbf` é um arquivo intermediário criado apenas durante o processamento. Ele é removido automaticamente ao final de cada conversão para economizar espaço.

### Retry e robustez
O download possui **3 tentativas automáticas** com intervalo de 5 segundos, e verifica o tamanho mínimo do arquivo baixado para detectar downloads corrompidos.

### Idempotência
Re-executar o script é seguro: arquivos já convertidos (`csv` + `csv.gz`) são detectados e pulados automaticamente.

---

## 📄 Licença

Este projeto é distribuído sob a licença **MIT**.

Os **dados** são de domínio público, disponibilizados pelo Ministério da Saúde do Brasil via DATASUS, conforme a [Lei de Acesso à Informação (Lei nº 12.527/2011)](http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm).

---

<div align="center">

**Desenvolvido para fins de pesquisa e análise de dados em saúde pública**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://www.python.org/)
[![DATASUS](https://img.shields.io/badge/Fonte-DATASUS%2FSINAN-green)](https://datasus.saude.gov.br/)
[![Licença](https://img.shields.io/badge/Licen%C3%A7a-MIT-orange)](LICENSE)

</div>
