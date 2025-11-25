import phonenumbers
from phonenumbers import carrier
from phonenumbers.phonenumberutil import number_type, PhoneNumberType
import pandas as pd
from tqdm import tqdm
import os
import time

# --- Configurações ---
INPUT_FOLDER = "arquivos_de_entrada"  # Pasta com seus arquivos Excel
OUTPUT_FOLDER = "arquivos_com_operadora"  # Pasta para os arquivos processados
# A coluna com os números de telefone deve conter "telefone" no nome
TELEFONE_COLUMN_HINT = "telefone"


def verificar_operadora(numero):
    """
    Verifica a operadora de um número de telefone, corrigindo a lógica
    para números fixos de 10 dígitos e celulares incompletos.
    """
    if pd.isna(numero) or str(numero).strip() == "":
        return ""

    # Garante que o número é tratado como string para evitar perdas de precisão (notação científica)
    numero_str = str(numero).strip()

    # Verifica se já tem código do país (+55)
    if numero_str.startswith('+'):
        numero_completo = numero_str
        # Extrai apenas os dígitos para validação
        numero_limpo = ''.join(filter(str.isdigit, numero_str[1:]))
    else:
        # Remove tudo que não for dígito
        numero_limpo = ''.join(filter(str.isdigit, numero_str))
        numero_completo = "+55" + numero_limpo

    # O número DEVE ter pelo menos 10 dígitos (DDD + 8 dígitos) para ser brasileiro
    if len(numero_limpo) < 10:
        return "Inválido (Curto)"

    try:
        # --- PRIMEIRA TENTATIVA: Tenta parsear o número como está (10 dígitos fixo ou 11 dígitos móvel) ---
        parsed_number = phonenumbers.parse(numero_completo, "BR")

        # Se for válido, é o número que vamos usar.
        if phonenumbers.is_valid_number(parsed_number):
            pass # Número validado com sucesso

        # --- SEGUNDA TENTATIVA: Se for 10 dígitos e inválido, tenta a correção do '9' ---
        # Isso ocorre se o número for um celular incompleto (ex: 7988888888, deveria ser 79988888888)
        elif len(numero_limpo) == 10 and numero_limpo[2] != '9':
            # Tenta adicionar o '9' depois do DDD
            numero_limpo_corrigido = numero_limpo[:2] + '9' + numero_limpo[2:]
            numero_completo_corrigido = "+55" + numero_limpo_corrigido
            parsed_number_corrigido = phonenumbers.parse(numero_completo_corrigido, "BR")

            if phonenumbers.is_valid_number(parsed_number_corrigido):
                parsed_number = parsed_number_corrigido  # Usa o número corrigido
            else:
                return "Inválido (Corrigido Falhou)"  # Nem o original, nem o corrigido funcionaram

        else:
            return "Inválido (Formato)"  # Não é válido e não é um 10-dígitos para tentar o '9'

    except Exception as e:
        # Erro de parsing (DDD inexistente, etc.)
        return "Inválido (Erro Parsing)"

    # --- VERIFICAÇÃO DE OPERADORA (O código abaixo não foi alterado, pois estava correto) ---
    operadora = carrier.name_for_number(parsed_number, "pt")

    if not operadora:
        n_type = number_type(parsed_number)
        if n_type == PhoneNumberType.FIXED_LINE:
            return "Linha Fixa"
        elif n_type == PhoneNumberType.MOBILE:
            return "Celular (Operadora não identificada)"
        return "Operadora Não Encontrada"

    return operadora


def processar_arquivo_excel(caminho_arquivo):
    """Lê um arquivo Excel, processa os números de telefone e salva o resultado."""
    print(f"\nProcessando arquivo: {os.path.basename(caminho_arquivo)}")

    try:
        # Adicione o argumento 'dtype=str' para ler as colunas de dados críticos como strings
        df = pd.read_excel(caminho_arquivo, dtype=str) 
    except Exception as e:
        print(f"Erro ao ler o arquivo {os.path.basename(caminho_arquivo)}: {e}")
        return

    # ... (Resto da função permanece o mesmo)
    coluna_telefone = None
    for col in df.columns:
        if "telefone".lower() in str(col).lower():
            coluna_telefone = col
            break

    if not coluna_telefone:
        print(f"Aviso: Coluna 'telefone' não encontrada no arquivo.")
        return

    total_numeros = len(df)
    df['Operadora'] = ""

    print("Iniciando a consulta...")
    for i in tqdm(range(total_numeros), desc="Consultando Números"):
        numero = df.loc[i, coluna_telefone]
        df.loc[i, 'Operadora'] = verificar_operadora(numero)

    df['Operadora'] = df['Operadora'].astype(str)

    nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
    nome_saida = f"{nome_base}_com_operadora{extensao}"
    caminho_saida = os.path.join(OUTPUT_FOLDER, nome_saida)

    try:
        df.to_excel(caminho_saida, index=False)
        print(f"Concluído! Arquivo salvo em: {caminho_saida}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo de saída: {e}")
        print("Verifique se o arquivo não está aberto e tente novamente.")



def main():
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    arquivos_excel = [os.path.join(INPUT_FOLDER, f) for f in os.listdir(INPUT_FOLDER) if f.endswith(('.xlsx', '.xls'))]

    if not arquivos_excel:
        print(f"Nenhum arquivo Excel encontrado na pasta '{INPUT_FOLDER}'.")
        return

    print(f"Encontrados {len(arquivos_excel)} arquivos para processar.")

    for arquivo in arquivos_excel:
        processar_arquivo_excel(arquivo)

    print("\nTodos os arquivos foram processados!")


if __name__ == "__main__":
    main()