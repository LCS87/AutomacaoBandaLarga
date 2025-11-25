# Importa as bibliotecas necessárias
import os
import pandas as pd
import requests
import time
from datetime import datetime
import logging
from tqdm import tqdm
import concurrent.futures
from threading import Semaphore
import json
import sys

# ================== CONFIGURAÇÕES AJUSTADAS ==================
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
INPUT_FOLDER = os.path.join(script_dir, "Consultar")
OUTPUT_FOLDER = os.path.join(script_dir, "Consultado")
# ALTERAÇÃO CRÍTICA: Ajustado para o nome da coluna nos arquivos de entrada
CNPJ_COLUMN = "DOCUMENTO" 
LOGS_FOLDER = os.path.join(script_dir, "Dados do cokpit/Logs")
CACHE_FILE = os.path.join(LOGS_FOLDER, "cnpj_cache.json")
API_KEY = "" # coloque sua chave de api aqui

# Limite de consultas: 900 por minuto
MAX_REQUESTS_PER_MINUTE = 900
RATE_LIMIT_INTERVAL = 60.0 / MAX_REQUESTS_PER_MINUTE

# Cria pastas se não existirem
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

# Configura o sistema de log
logging.basicConfig(
    filename=os.path.join(LOGS_FOLDER, f"cnpj_consultas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Semáforo para controlar limite de requisições
api_semaphore = Semaphore(MAX_REQUESTS_PER_MINUTE)

# Carrega cache
cnpj_cache = {}
if os.path.exists(CACHE_FILE):
    try:
        # Usando 'with' para garantir que o arquivo seja fechado
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            cnpj_cache = json.load(f)
    except:
        logger.warning("Erro ao carregar cache, iniciando novo")

def save_cache():
    try:
        # Usando 'with' e codificação utf-8
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cnpj_cache, f, indent=4)
    except Exception as e:
        logger.error(f"Erro ao salvar cache: {str(e)}")

# ================== FUNÇÕES BASE ==================
def format_cnpj(cnpj):
    if pd.isna(cnpj):
        return None
    # Garante que o CNPJ é uma string, crucial para lidar com notação científica
    cnpj = str(cnpj).strip()
    # Remove todos os caracteres não-dígitos
    return ''.join(filter(str.isdigit, cnpj))

def consulta_invertexto(cnpj):
    """Consulta a API do Invertexto usando chave e limite de taxa."""
    url = f"https://api.blablabla.com/v1/cnpj/{cnpj}"  # coloque o link de sua API de consulta aqui
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    # Controle de limite de requisições
    with api_semaphore:
        try:
            r = requests.get(url, headers=headers, timeout=10)
            
            # Aguarda o tempo necessário para respeitar o limite de taxa
            time.sleep(RATE_LIMIT_INTERVAL) 
            
            if r.status_code == 200:
                return r.json()
            else:
                logger.error(f"Erro {r.status_code} ao consultar {cnpj}: {r.text}")
        except Exception as e:
            logger.error(f"Erro de conexão/timeout ao consultar {cnpj}: {str(e)}")
    return None

def first_value(data, keys):
    for k in keys:
        if v := data.get(k):
            return v
    return ""

def parse_empresa_data(data):
    empresa = {
        "CNPJ": first_value(data, ["cnpj"]),
        "RazaoSocial": first_value(data, ["razao_social"]) or "NÃO ENCONTRADO",
        "NomeFantasia": first_value(data, ["nome_fantasia"]),
        "SituacaoCadastral": data.get("situacao", {}).get("nome"),
        "DataAbertura": first_value(data, ["data_inicio"]),
        "NaturezaJuridica": first_value(data, ["natureza_juridica"]),
        "CapitalSocial": first_value(data, ["capital_social"]),
        "Fonte": "Invertexto"
    }

    endereco = data.get("endereco", {})
    empresa.update({
        "CEP": endereco.get("cep"),
        "Logradouro": endereco.get("logradouro"),
        "Numero": endereco.get("numero"),
        "Complemento": endereco.get("complemento"),
        "Bairro": endereco.get("bairro"),
        "Municipio": endereco.get("municipio"),
        "UF": endereco.get("uf")
    })
    empresa["EnderecoCompleto"] = ", ".join(filter(None, [
        empresa["Logradouro"], empresa["Numero"], empresa["Complemento"],
        empresa["Bairro"], empresa["Municipio"], empresa["UF"], empresa["CEP"]
    ]))

    # Extrai informações de contato (telefone e e-mail) de forma robusta
    telefones_encontrados = []
    
    if telefone1 := data.get("telefone1"):
        telefones_encontrados.append(str(telefone1).strip())
    if telefone2 := data.get("telefone2"):
        telefones_encontrados.append(str(telefone2).strip())
        
    if telefone_endereco := endereco.get("telefone"):
        telefones_encontrados.append(str(telefone_endereco).strip())

    if telefones_encontrados:
        empresa["Telefone"] = " / ".join(sorted(list(set(telefones_encontrados))))
    else:
        empresa["Telefone"] = ""

    empresa["Email"] = data.get("email") or ""
    
    # Atividade principal
    if isinstance(data.get("atividade_principal"), dict):
        empresa["AtividadePrincipal"] = data["atividade_principal"].get("descricao", "")
    else:
        empresa["AtividadePrincipal"] = ""

    return empresa

# ================== LÓGICA DE CONSULTA ==================
def get_empresa_data(cnpj):
    # Caso o CNPJ tenha falhado na formatação
    if not cnpj:
        return {"CNPJ": "N/A", "RazaoSocial": "CNPJ INVÁLIDO/NÃO ENCONTRADO", "Fonte": "Erro"}

    # Busca no cache
    if cnpj in cnpj_cache and cnpj_cache[cnpj]['RazaoSocial'] != 'NÃO ENCONTRADO':
        return cnpj_cache[cnpj]

    # Consulta a API
    data = consulta_invertexto(cnpj)
    if data:
        parsed = parse_empresa_data(data)
        cnpj_cache[cnpj] = parsed
        return parsed

    # Falha na consulta
    empty = {"CNPJ": cnpj, "RazaoSocial": "NÃO ENCONTRADO", "Fonte": "Invertexto"}
    cnpj_cache[cnpj] = empty
    return empty

def processar_cnpjs(cnpjs):
    resultados = []
    novos_cnpjs = [c for c in cnpjs if c and c not in cnpj_cache]
    
    # Adiciona resultados já cacheados
    for c in cnpjs:
        if c in cnpj_cache:
            resultados.append(cnpj_cache[c])

    if novos_cnpjs:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_cnpj = {executor.submit(get_empresa_data, c): c for c in novos_cnpjs}
            
            for i, future in enumerate(tqdm(concurrent.futures.as_completed(future_to_cnpj), total=len(novos_cnpjs), desc="Consultando CNPJs")):
                cnpj = future_to_cnpj[future]
                try:
                    resultados.append(future.result())
                except Exception as e:
                    logger.error(f"Erro ao processar CNPJ {cnpj}: {str(e)}")
                    resultados.append({"CNPJ": cnpj, "RazaoSocial": "ERRO NA CONSULTA", "Fonte": "Erro"})
                
                if (i + 1) % 100 == 0:
                    save_cache()
                    
    return resultados

# ================== PROCESSAMENTO DE PLANILHAS ==================
def processar_planilha(arquivo):
    try:
        logger.info(f"Iniciando processamento do arquivo: {arquivo}")
        
        # Leitura do arquivo: Força a coluna CNPJ_COLUMN a ser lida como string para evitar notação científica
        if arquivo.endswith('.xlsx'):
            df = pd.read_excel(arquivo, dtype={CNPJ_COLUMN: str})
        else:
            # Assumindo que os CSVs usam ponto e vírgula como delimitador (com base nos seus snippets)
            df = pd.read_csv(arquivo, sep=';', dtype={CNPJ_COLUMN: str})
            
        if CNPJ_COLUMN not in df.columns:
            logger.error(f"Arquivo {arquivo} não contém coluna '{CNPJ_COLUMN}'")
            return None

        # Formata e filtra CNPJs válidos (14 dígitos)
        df['CNPJ_FORMATADO'] = df[CNPJ_COLUMN].apply(format_cnpj)
        cnpjs = [c for c in df['CNPJ_FORMATADO'].unique() if c and len(c) == 14]
        
        if not cnpjs:
            logger.error(f"Nenhum CNPJ válido (14 dígitos) encontrado em {arquivo}")
            return None

        print(f"  Total de CNPJs únicos para consulta: {len(cnpjs)}")

        # Consulta a API
        resultados = processar_cnpjs(cnpjs)
        
        # Cria o DataFrame de resultados da consulta e mescla ao original
        df_resultado_consulta = pd.DataFrame(resultados)
        
        df_final = pd.merge(
            df, 
            df_resultado_consulta, 
            left_on='CNPJ_FORMATADO', 
            right_on='CNPJ', 
            how='left'
        )
        
        # Limpeza e reordenação
        df_final = df_final.drop(columns=['CNPJ_FORMATADO'])
        
        # Lista de colunas desejadas e ordem
        colunas_ordenadas = [
            'SMARTCODE', 'DOCUMENTO', 'CNPJ', 'RazaoSocial', 'NomeFantasia', 'SituacaoCadastral', 
            'DataAbertura', 'NaturezaJuridica', 'CapitalSocial', 'AtividadePrincipal',
            'CEP', 'Logradouro', 'Numero', 'Complemento', 'Bairro', 'Municipio', 'UF', 'EnderecoCompleto',
            'TELEFONE', 'Telefone', 'Email', 'ENRIQUECIMENTO', 'FLAGDISPO', 'TIPO_BLOQUEIO_TELEFONE', 'Fonte'
        ]
        
        df_final = df_final.reindex(columns=[c for c in colunas_ordenadas if c in df_final.columns])

        # Salva o arquivo final
        nome_base = os.path.splitext(os.path.basename(arquivo))[0]
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(OUTPUT_FOLDER, f"{nome_base}_consultado_{data_hora}.xlsx")
        df_final.to_excel(output_file, index=False)
        
        # Identifica e salva CNPJs que falharam na consulta
        falhas = df_resultado_consulta[
            df_resultado_consulta['RazaoSocial'].isin(['NÃO ENCONTRADO', 'ERRO NA CONSULTA'])
        ]['CNPJ'].tolist()

        if falhas:
            with open(os.path.join(OUTPUT_FOLDER, f"{nome_base}_falhas_{data_hora}.txt"), 'w', encoding='utf-8') as f:
                f.write("\n".join(falhas))

        logger.info(f"Arquivo processado: {arquivo}")
        print(f"  Resultados salvos em: {output_file}")
        return output_file

    except Exception as e:
        logger.error(f"Erro ao processar {arquivo}: {str(e)}", exc_info=True)
        return None

# ================== MAIN ==================
def main():
    print("\n" + "="*50)
    print("SISTEMA DE CONSULTA DE CNPJ EM MASSA (INVERTEXTO)")
    print("="*50 + "\n")
    
    # Filtra arquivos CSV e XLSX
    arquivos = [os.path.join(INPUT_FOLDER, f) for f in os.listdir(INPUT_FOLDER) if f.endswith(('.xlsx', '.csv')) and not f.startswith('~')]
    
    if not arquivos:
        print(f"Nenhum arquivo encontrado em {INPUT_FOLDER}. Certifique-se de que seus arquivos .xlsx/csv estão lá.")
        return
    print(f"\nArquivos encontrados: {len(arquivos)}")
    
    for arquivo in arquivos:
        print(f"\nProcessando: {os.path.basename(arquivo)}")
        inicio = time.time()
        processar_planilha(arquivo)
        print(f"Concluído em {time.time() - inicio:.2f} segundos")
    
    save_cache()
    print("\nProcessamento concluído!")

if __name__ == "__main__":
    main()