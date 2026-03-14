import pandas as pd
import re
import os
import glob
import sys
import logging
from pathlib import Path
from collections import defaultdict

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Usa o diretório do próprio script como base (portável entre máquinas)
SCRIPT_DIR = Path(__file__).parent
PASTA_DADOS_ENTRADA = SCRIPT_DIR / "dados_prospect"
PASTA_DADOS_SAIDA = PASTA_DADOS_ENTRADA

PASTA_DADOS_ENTRADA.mkdir(exist_ok=True)

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "separador.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- FUNÇÕES DE SUPORTE ---

def extrair_analise_credito(enriquecimento: str) -> str:
    """Extrai o valor após 'ANALISE_CREDITO=' e remove caracteres extras."""
    if pd.isna(enriquecimento) or not isinstance(enriquecimento, str):
        return "N/A"
    match = re.search(r"ANALISE\_CREDITO\=(.*?)(?:\||$)", enriquecimento)
    if match:
        extracted = match.group(1).strip().rstrip('.')
        return extracted.strip()
    return "N/A"


def classificar_cliente(descricao: str) -> str:
    """Classifica a descrição em 'MEI', 'DEMAIS' ou 'DESCARTAR'."""
    if not isinstance(descricao, str):
        return "DESCARTAR"

    mei_aprovados = {
        "Cliente MEI com alta probabilidade de aprovação",
        "Cliente MEI com média probabilidade de aprovação",
        "Cliente MEI com altissíma probabilidade de aprovação"
    }
    demais_clientes_aprovados = {
        "Cliente com altissíma probabilidade de aprovação",
        "Cliente com média probabilidade de aprovação",
        "Cliente aprovado até R$ 1700.00",
        "Cliente aprovado até R$ 3000.00",
        "Cliente aprovado até R$ 4000.00",
        "Cliente aprovado até R$ 1500.00",
        "Cliente aprovado com mais de R$ 5000,00",
        "Cliente aprovado até R$ 5000.00"
    }

    if descricao in mei_aprovados:
        return "MEI"
    if descricao in demais_clientes_aprovados:
        return "DEMAIS"
    return "DESCARTAR"


def salvar_dataframe_excel(df: pd.DataFrame, caminho_saida: Path, nome_aba: str, df_resumo: pd.DataFrame):
    """Salva DataFrame em Excel, com fallback para openpyxl e depois CSV."""
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)

    if 'CHAVE_AGRUPAMENTO' in df.columns:
        df = df.drop(columns=['CHAVE_AGRUPAMENTO'])

    for engine in ('xlsxwriter', 'openpyxl'):
        try:
            with pd.ExcelWriter(str(caminho_saida), engine=engine) as writer:
                df.to_excel(writer, sheet_name=nome_aba, index=False)
                df_resumo.to_excel(writer, sheet_name="Resumo_Contagem", index=False)
            logger.info(f"{caminho_saida.name} criado com {len(df)} linhas (engine: {engine}).")
            return
        except Exception as e:
            logger.warning(f"Engine {engine} falhou: {e}")

    # Fallback CSV
    try:
        csv_saida = caminho_saida.with_suffix('.csv')
        df.to_csv(csv_saida, index=False, sep=';', encoding='utf-8-sig')
        df_resumo.to_csv(caminho_saida.with_name(caminho_saida.stem + "_resumo.csv"),
                         index=False, sep=';', encoding='utf-8-sig')
        logger.warning(f"Fallback CSV: {csv_saida.name}")
    except Exception as e:
        logger.error(f"Erro ao escrever {caminho_saida.name}: {e}")


def extrair_sigla_uf(nome_arquivo_base: str) -> str:
    """Extrai a sigla da UF a partir do nome do arquivo."""
    if not isinstance(nome_arquivo_base, str):
        return "UF_INDEFINIDA"
    m = re.search(r"\b([A-Z]{2})\b", nome_arquivo_base)
    if m:
        return m.group(1)
    m2 = re.search(r"[-_]\s*([A-Z]{2})\b", nome_arquivo_base)
    if m2:
        return m2.group(1)
    return "UF_INDEFINIDA"


def processar_arquivo_prospect(caminho_arquivo: Path):
    """Lê um CSV, classifica prospects em MEI/DEMAIS e salva em Excel."""
    nome_base = caminho_arquivo.stem
    uf_sigla = extrair_sigla_uf(nome_base)

    logger.info(f"Processando {nome_base} (UF: {uf_sigla})")

    try:
        df_origem = pd.read_csv(str(caminho_arquivo), delimiter=';', encoding='latin1')
    except Exception as e:
        logger.error(f"Erro ao ler {caminho_arquivo.name}: {e}")
        return

    if 'ENRIQUECIMENTO' not in df_origem.columns:
        logger.warning(f"Coluna 'ENRIQUECIMENTO' não encontrada em {caminho_arquivo.name}. Pulando.")
        return

    df_origem['CHAVE_AGRUPAMENTO'] = df_origem['ENRIQUECIMENTO'].apply(extrair_analise_credito)
    df_origem['CLASSIFICACAO'] = df_origem['CHAVE_AGRUPAMENTO'].apply(classificar_cliente)

    df_filtrado = df_origem[df_origem['CLASSIFICACAO'] != 'DESCARTAR'].copy()

    if df_filtrado.empty:
        logger.warning("Nenhum cliente elegível encontrado. Nenhum arquivo gerado.")
        return

    df_mei = df_filtrado[df_filtrado['CLASSIFICACAO'] == 'MEI'].drop(columns=['CLASSIFICACAO'])
    df_demais = df_filtrado[df_filtrado['CLASSIFICACAO'] == 'DEMAIS'].drop(columns=['CLASSIFICACAO'])

    if not df_mei.empty:
        resumo_mei = df_mei['CHAVE_AGRUPAMENTO'].value_counts().reset_index()
        resumo_mei.columns = ["Descrição ANALISE_CREDITO", "Quantidade de Linhas"]
        salvar_dataframe_excel(
            df_mei,
            PASTA_DADOS_SAIDA / f"Prospect {uf_sigla} - MEI.xlsx",
            "Prospect MEI",
            resumo_mei
        )

    if not df_demais.empty:
        resumo_demais = df_demais['CHAVE_AGRUPAMENTO'].value_counts().reset_index()
        resumo_demais.columns = ["Descrição ANALISE_CREDITO", "Quantidade de Linhas"]
        salvar_dataframe_excel(
            df_demais,
            PASTA_DADOS_SAIDA / f"Prospect {uf_sigla} - Demais clientes.xlsx",
            "Prospect Demais",
            resumo_demais
        )


# --- FUNÇÃO PRINCIPAL ---

def main():
    if not PASTA_DADOS_ENTRADA.is_dir():
        logger.error(f"Pasta de entrada não encontrada: {PASTA_DADOS_ENTRADA}")
        sys.exit(1)

    arquivos_csv = list(PASTA_DADOS_ENTRADA.glob("*.csv"))

    if not arquivos_csv:
        logger.warning(f"Nenhum arquivo .csv encontrado em: {PASTA_DADOS_ENTRADA}")
        sys.exit(0)

    logger.info(f"Encontrados {len(arquivos_csv)} arquivos. Iniciando processamento...")

    for caminho_arquivo in arquivos_csv:
        processar_arquivo_prospect(caminho_arquivo)

    logger.info(f"Processamento concluído. Saída em: {PASTA_DADOS_SAIDA.resolve()}")


if __name__ == "__main__":
    main()
