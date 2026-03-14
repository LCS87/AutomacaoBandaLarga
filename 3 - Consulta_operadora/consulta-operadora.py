import pandas as pd
from tqdm import tqdm
import os
import sys
import logging
from pathlib import Path

# Importa função compartilhada (evita duplicação com Script 4)
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.telefone import verificar_operadora

# --- Configurações ---
SCRIPT_DIR    = Path(__file__).parent
INPUT_FOLDER  = SCRIPT_DIR / "arquivos_de_entrada"
OUTPUT_FOLDER = SCRIPT_DIR / "arquivos_com_operadora"
TELEFONE_COLUMN_HINT = "telefone"

INPUT_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "consulta_operadora.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def processar_arquivo_excel(caminho_arquivo: Path):
    """Lê um arquivo Excel, identifica operadora de cada telefone e salva o resultado."""
    logger.info(f"Processando: {caminho_arquivo.name}")

    try:
        df = pd.read_excel(str(caminho_arquivo), dtype=str)
    except Exception as e:
        logger.error(f"Erro ao ler {caminho_arquivo.name}: {e}")
        return

    # Localiza coluna de telefone (case-insensitive)
    coluna_telefone = next(
        (col for col in df.columns if TELEFONE_COLUMN_HINT.lower() in col.lower()),
        None
    )

    if not coluna_telefone:
        logger.warning(f"Coluna '{TELEFONE_COLUMN_HINT}' não encontrada em {caminho_arquivo.name}.")
        return

    logger.info(f"Consultando {len(df)} números na coluna '{coluna_telefone}'...")

    # Usa .apply() ao invés de loop com .loc[] (muito mais rápido)
    df['Operadora'] = df[coluna_telefone].apply(verificar_operadora)
    df['Operadora'] = df['Operadora'].astype(str)

    nome_saida = f"{caminho_arquivo.stem}_com_operadora{caminho_arquivo.suffix}"
    caminho_saida = OUTPUT_FOLDER / nome_saida

    try:
        df.to_excel(str(caminho_saida), index=False)
        logger.info(f"Salvo em: {caminho_saida}")
    except Exception as e:
        logger.error(f"Erro ao salvar {nome_saida}: {e}. Verifique se o arquivo não está aberto.")


def main():
    arquivos = [f for f in INPUT_FOLDER.iterdir() if f.suffix in ('.xlsx', '.xls')]

    if not arquivos:
        logger.warning(f"Nenhum arquivo Excel encontrado em '{INPUT_FOLDER}'.")
        return

    logger.info(f"Encontrados {len(arquivos)} arquivos para processar.")

    for arquivo in arquivos:
        processar_arquivo_excel(arquivo)

    logger.info("Todos os arquivos foram processados.")


if __name__ == "__main__":
    main()
