"""
============================================================
  DATASUS SINAN-VIOL — Pipeline de Download e Conversão
  Violência Doméstica, Sexual e/ou Outras Violências
  Fonte: ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/
  Microdados: https://alpaca.quantilica.com/dados/sinan-viol
============================================================
  Saídas por arquivo:
    ├── VIOLBRXX.dbc       ← arquivo original comprimido
    ├── VIOLBRXX.csv       ← tabela plana (utf-8-sig, Excel-safe)
    └── VIOLBRXX.csv.gz    ← comprimido para ML / Big Data
  
  Relatório final:
    └── relatorio_execucao.txt
============================================================
  Dependências:
    pip install dbfread pandas datasus-dbc
============================================================
"""

import os
import sys
import time
import gzip
import shutil
import urllib.request
import datetime
import traceback

from dbfread import DBF
import pandas as pd
from datasus_dbc import decompress

# ─────────────────────────────────────────────
#  CONFIGURAÇÕES
# ─────────────────────────────────────────────

# Pasta base = mesma pasta onde este script está
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MAX_TENTATIVAS  = 3       # tentativas de download por arquivo
PAUSA_RETRY_SEG = 5       # segundos entre tentativas
ENCODING_DBF    = "iso-8859-1"   # padrão DATASUS
ENCODING_CSV    = "utf-8-sig"    # BOM → compatível com Excel
CHUNK_SIZE      = 8192    # bytes para leitura streaming (gz)

URLS = [
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR09.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR10.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR11.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR12.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR13.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR14.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR15.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR16.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR17.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR18.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR19.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR20.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR21.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR22.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR23.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/VIOLBR24.dbc",
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/PRELIM/VIOLBR25.dbc",
]


# ─────────────────────────────────────────────
#  UTILITÁRIOS
# ─────────────────────────────────────────────

def tamanho_humano(bytes_: int) -> str:
    """Converte bytes para string legível (KB / MB / GB)."""
    for unidade in ["B", "KB", "MB", "GB"]:
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unidade}"
        bytes_ /= 1024
    return f"{bytes_:.1f} TB"


def cabecalho(texto: str) -> str:
    linha = "─" * 60
    return f"\n{linha}\n  {texto}\n{linha}"


def log(msg: str, arquivo_log=None) -> None:
    """Imprime no terminal e opcionalmente escreve no arquivo de log."""
    print(msg)
    if arquivo_log:
        arquivo_log.write(msg + "\n")
        arquivo_log.flush()


# ─────────────────────────────────────────────
#  ETAPAS DO PIPELINE
# ─────────────────────────────────────────────

def baixar_dbc(url: str, destino: str, logger=None) -> bool:
    """
    Download do .dbc com retry automático.
    Retorna True em sucesso, False em falha definitiva.
    """
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            log(f"  ↓ Download (tentativa {tentativa}/{MAX_TENTATIVAS}): {url}", logger)
            urllib.request.urlretrieve(url, destino)

            tamanho = os.path.getsize(destino)
            if tamanho < 200:
                raise ValueError(f"Arquivo suspeito — apenas {tamanho} bytes.")

            log(f"  ✔ Download OK  [{tamanho_humano(tamanho)}]", logger)
            return True

        except Exception as exc:
            log(f"  ✘ Tentativa {tentativa} falhou: {exc}", logger)
            if os.path.exists(destino):
                os.remove(destino)
            if tentativa < MAX_TENTATIVAS:
                log(f"  ⏳ Aguardando {PAUSA_RETRY_SEG}s antes de tentar novamente…", logger)
                time.sleep(PAUSA_RETRY_SEG)

    return False


def descomprimir_dbc(caminho_dbc: str, caminho_dbf: str, logger=None) -> bool:
    """Descomprime .dbc → .dbf usando datasus_dbc."""
    try:
        log("  ⚙  Descomprimindo DBC → DBF…", logger)
        decompress(caminho_dbc, caminho_dbf)
        log(f"  ✔ DBF gerado  [{tamanho_humano(os.path.getsize(caminho_dbf))}]", logger)
        return True
    except Exception as exc:
        log(f"  ✘ Erro na descompressão: {exc}", logger)
        return False


def converter_para_csv_e_gz(
    caminho_dbf: str,
    caminho_csv: str,
    caminho_gz:  str,
    logger=None
) -> dict:
    """
    Lê o DBF e gera:
      - CSV plano  (utf-8-sig)
      - CSV.GZ     (comprimido, mesmo conteúdo)

    Retorna dict com metadados ou None em erro.
    """
    try:
        log("  📖 Lendo DBF…", logger)
        dbf_dados = DBF(caminho_dbf, encoding=ENCODING_DBF)
        df = pd.DataFrame(iter(dbf_dados))

        n_linhas, n_colunas = df.shape
        log(f"  ✔ {n_linhas:,} registros × {n_colunas} colunas", logger)

        # ── CSV plano ──────────────────────────────────────
        log("  💾 Salvando CSV…", logger)
        df.to_csv(caminho_csv, index=False, encoding=ENCODING_CSV)
        tam_csv = os.path.getsize(caminho_csv)
        log(f"  ✔ CSV salvo  [{tamanho_humano(tam_csv)}]", logger)

        # ── CSV.GZ (comprime o CSV já gerado) ─────────────
        log("  🗜  Comprimindo → CSV.GZ…", logger)
        with open(caminho_csv, "rb") as f_in, \
             gzip.open(caminho_gz, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        tam_gz = os.path.getsize(caminho_gz)
        razao  = (1 - tam_gz / tam_csv) * 100 if tam_csv else 0
        log(f"  ✔ CSV.GZ salvo [{tamanho_humano(tam_gz)}]  "
            f"(redução: {razao:.1f}%)", logger)

        return {
            "registros": n_linhas,
            "colunas":   n_colunas,
            "tam_csv":   tam_csv,
            "tam_gz":    tam_gz,
            "razao_gz":  razao,
        }

    except Exception as exc:
        log(f"  ✘ Erro na conversão: {exc}", logger)
        log(traceback.format_exc(), logger)
        return None


def gerar_relatorio_leitura(resultados: list, logger=None) -> None:
    """
    Demonstra como carregar os arquivos gerados em Python
    e exibe um resumo de leitura por arquivo.
    """
    log(cabecalho("DEMONSTRAÇÃO DE LEITURA DOS ARQUIVOS"), logger)

    for r in resultados:
        if r["status"] != "OK":
            continue

        nome = r["nome_base"]
        log(f"\n  [{nome}]", logger)

        # Leitura CSV
        caminho_csv = r["caminho_csv"]
        if os.path.exists(caminho_csv):
            try:
                df_csv = pd.read_csv(caminho_csv, encoding=ENCODING_CSV, nrows=3)
                log(f"    pd.read_csv('{nome}.csv')  → shape {df_csv.shape}", logger)
            except Exception as exc:
                log(f"    Erro ao ler CSV: {exc}", logger)

        # Leitura CSV.GZ
        caminho_gz = r["caminho_gz"]
        if os.path.exists(caminho_gz):
            try:
                df_gz = pd.read_csv(caminho_gz, compression="gzip",
                                    encoding=ENCODING_CSV, nrows=3)
                log(f"    pd.read_csv('{nome}.csv.gz', compression='gzip')  "
                    f"→ shape {df_gz.shape}", logger)
            except Exception as exc:
                log(f"    Erro ao ler CSV.GZ: {exc}", logger)


# ─────────────────────────────────────────────
#  PIPELINE PRINCIPAL
# ─────────────────────────────────────────────

def main():
    inicio_geral = time.time()
    timestamp    = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_relatorio = os.path.join(BASE_DIR, f"relatorio_execucao_{timestamp}.txt")

    resultados: list[dict] = []

    with open(caminho_relatorio, "w", encoding="utf-8") as log_file:

        log(cabecalho("DATASUS SINAN-VIOL — Pipeline DBC → CSV / CSV.GZ"), log_file)
        log(f"  Início : {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", log_file)
        log(f"  Pasta  : {BASE_DIR}", log_file)
        log(f"  Total  : {len(URLS)} arquivos\n", log_file)

        for idx, url in enumerate(URLS, start=1):
            nome_dbc  = url.split("/")[-1]           # ex: VIOLBR09.dbc
            nome_base = nome_dbc.replace(".dbc", "")  # ex: VIOLBR09

            caminho_dbc = os.path.join(BASE_DIR, nome_dbc)
            caminho_dbf = os.path.join(BASE_DIR, f"{nome_base}.dbf")  # temp
            caminho_csv = os.path.join(BASE_DIR, f"{nome_base}.csv")
            caminho_gz  = os.path.join(BASE_DIR, f"{nome_base}.csv.gz")

            resultado = {
                "idx":        idx,
                "nome_base":  nome_base,
                "url":        url,
                "caminho_dbc": caminho_dbc,
                "caminho_csv": caminho_csv,
                "caminho_gz":  caminho_gz,
                "status":     "PENDENTE",
                "registros":  0,
                "colunas":    0,
                "tam_dbc":    0,
                "tam_csv":    0,
                "tam_gz":     0,
                "razao_gz":   0.0,
                "erro":       "",
                "duracao_s":  0.0,
            }

            log(cabecalho(f"[{idx:02d}/{len(URLS)}]  {nome_dbc}"), log_file)
            inicio = time.time()

            # ── Pula se CSV e GZ já existem (idempotência) ─────────
            if os.path.exists(caminho_csv) and os.path.exists(caminho_gz):
                log("  ⏭  CSV e CSV.GZ já existem — pulando.", log_file)
                resultado["status"] = "PULADO"
                resultado["tam_dbc"] = os.path.getsize(caminho_dbc) if os.path.exists(caminho_dbc) else 0
                resultado["tam_csv"] = os.path.getsize(caminho_csv)
                resultado["tam_gz"]  = os.path.getsize(caminho_gz)
                resultados.append(resultado)
                continue

            # ── 1. Download .dbc ────────────────────────────────────
            if not os.path.exists(caminho_dbc):
                ok = baixar_dbc(url, caminho_dbc, log_file)
                if not ok:
                    resultado["status"] = "ERRO_DOWNLOAD"
                    resultado["erro"]   = "Falha no download após todas as tentativas."
                    resultado["duracao_s"] = round(time.time() - inicio, 2)
                    resultados.append(resultado)
                    continue
            else:
                log(f"  ⏭  DBC já existe no disco — pulando download.", log_file)

            resultado["tam_dbc"] = os.path.getsize(caminho_dbc)

            # ── 2. Descompressão .dbc → .dbf (temporário) ──────────
            ok = descomprimir_dbc(caminho_dbc, caminho_dbf, log_file)
            if not ok:
                resultado["status"] = "ERRO_DECOMPRESS"
                resultado["erro"]   = "Falha na descompressão DBC→DBF."
                resultado["duracao_s"] = round(time.time() - inicio, 2)
                resultados.append(resultado)
                continue

            # ── 3. Conversão DBF → CSV + CSV.GZ ────────────────────
            meta = converter_para_csv_e_gz(caminho_dbf, caminho_csv, caminho_gz, log_file)

            # Remove DBF temporário independente do resultado
            if os.path.exists(caminho_dbf):
                os.remove(caminho_dbf)
                log("  🗑  DBF temporário removido.", log_file)

            if meta is None:
                resultado["status"] = "ERRO_CONVERSAO"
                resultado["erro"]   = "Falha na conversão DBF→CSV/GZ."
            else:
                resultado.update({
                    "status":   "OK",
                    "registros": meta["registros"],
                    "colunas":   meta["colunas"],
                    "tam_csv":   meta["tam_csv"],
                    "tam_gz":    meta["tam_gz"],
                    "razao_gz":  meta["razao_gz"],
                })

            resultado["duracao_s"] = round(time.time() - inicio, 2)
            resultados.append(resultado)
            log(f"  ⏱  Tempo do arquivo: {resultado['duracao_s']:.1f}s", log_file)

        # ── DEMONSTRAÇÃO DE LEITURA ─────────────────────────────────
        gerar_relatorio_leitura(resultados, log_file)

        # ── RELATÓRIO FINAL ─────────────────────────────────────────
        log(cabecalho("RELATÓRIO FINAL DE EXECUÇÃO"), log_file)

        ok_lista    = [r for r in resultados if r["status"] == "OK"]
        pulados     = [r for r in resultados if r["status"] == "PULADO"]
        erros       = [r for r in resultados if r["status"].startswith("ERRO")]
        total_reg   = sum(r["registros"] for r in ok_lista)
        total_csv   = sum(r["tam_csv"]   for r in resultados if r["tam_csv"])
        total_gz    = sum(r["tam_gz"]    for r in resultados if r["tam_gz"])
        total_dbc   = sum(r["tam_dbc"]   for r in resultados if r["tam_dbc"])
        duracao_total = round(time.time() - inicio_geral, 2)

        linhas = [
            "",
            f"  {'ARQ':<12} {'STATUS':<16} {'REGISTROS':>10} {'COLUNAS':>8} "
            f"{'DBC':>9} {'CSV':>9} {'GZ':>9} {'RED%':>6} {'TEMPO':>7}",
            f"  {'─'*12} {'─'*16} {'─'*10} {'─'*8} "
            f"{'─'*9} {'─'*9} {'─'*9} {'─'*6} {'─'*7}",
        ]
        for r in resultados:
            linhas.append(
                f"  {r['nome_base']:<12} {r['status']:<16} "
                f"{r['registros']:>10,} {r['colunas']:>8} "
                f"{tamanho_humano(r['tam_dbc']):>9} "
                f"{tamanho_humano(r['tam_csv']):>9} "
                f"{tamanho_humano(r['tam_gz']):>9} "
                f"{r['razao_gz']:>5.1f}% "
                f"{r['duracao_s']:>6.1f}s"
            )

        linhas += [
            f"  {'─'*100}",
            f"  TOTAIS:  {len(ok_lista)} OK  |  {len(pulados)} pulados  |  {len(erros)} erros",
            f"  Registros totais processados : {total_reg:,}",
            f"  Espaço DBC  : {tamanho_humano(total_dbc)}",
            f"  Espaço CSV  : {tamanho_humano(total_csv)}",
            f"  Espaço GZ   : {tamanho_humano(total_gz)}",
            f"  Tempo total : {duracao_total:.1f}s ({duracao_total/60:.1f} min)",
            f"  Fim         : {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        ]

        if erros:
            linhas += ["", "  ❌ ARQUIVOS COM ERRO:"]
            for r in erros:
                linhas.append(f"     • {r['nome_base']} [{r['status']}] — {r['erro']}")

        for linha in linhas:
            log(linha, log_file)

        log(f"\n  📄 Relatório salvo em: {caminho_relatorio}", log_file)
        log(cabecalho("FIM DO PIPELINE"), log_file)


# ─────────────────────────────────────────────
#  SNIPPETS PARA USO EM ANÁLISE / ML
# ─────────────────────────────────────────────
SNIPPETS_ML = '''
# ═══════════════════════════════════════════════════════════
#  SNIPPETS — Como usar os arquivos gerados em Python / ML
# ═══════════════════════════════════════════════════════════

import pandas as pd
import glob, os

# ── 1. Carregar um único CSV ──────────────────────────────
df = pd.read_csv("VIOLBR23.csv", encoding="utf-8-sig")

# ── 2. Carregar um único CSV.GZ (memória eficiente) ───────
df = pd.read_csv("VIOLBR23.csv.gz", compression="gzip", encoding="utf-8-sig")

# ── 3. Concatenar TODOS os anos em um único DataFrame ─────
arquivos_gz = sorted(glob.glob("VIOLBR*.csv.gz"))
df_total = pd.concat(
    [pd.read_csv(f, compression="gzip", encoding="utf-8-sig") for f in arquivos_gz],
    ignore_index=True
)
print(df_total.shape)

# ── 4. Leitura em chunks (datasets muito grandes) ─────────
chunks = pd.read_csv(
    "VIOLBR23.csv.gz",
    compression="gzip",
    encoding="utf-8-sig",
    chunksize=100_000      # processa 100k linhas por vez
)
for chunk in chunks:
    pass  # seu processamento aqui

# ── 5. Uso com Scikit-learn / Deep Learning ───────────────
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv("VIOLBR23.csv.gz", compression="gzip", encoding="utf-8-sig")

# Remove colunas totalmente nulas
df.dropna(axis=1, how="all", inplace=True)

# Codifica variáveis categóricas (exemplo)
for col in df.select_dtypes(include="object").columns:
    df[col] = LabelEncoder().fit_transform(df[col].astype(str))

X = df.drop(columns=["TP_VIOL"])  # ajuste para sua coluna alvo
y = df["TP_VIOL"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ── 6. TensorFlow / Keras (Deep Learning) ─────────────────
# import tensorflow as tf
# dataset = tf.data.experimental.make_csv_dataset(
#     "VIOLBR23.csv.gz",
#     batch_size=256,
#     compression_type="GZIP"
# )

# ── 7. PyTorch Dataset personalizado ──────────────────────
# from torch.utils.data import Dataset
# class ViolDataset(Dataset):
#     def __init__(self, path):
#         self.df = pd.read_csv(path, compression="gzip", encoding="utf-8-sig")
#     def __len__(self):  return len(self.df)
#     def __getitem__(self, idx): return self.df.iloc[idx].values
'''

if __name__ == "__main__":
    main()

    # Exibe os snippets ao final
    print("\n" + "═" * 60)
    print("  SNIPPETS PARA ANÁLISE / ML")
    print("═" * 60)
    print(SNIPPETS_ML)
