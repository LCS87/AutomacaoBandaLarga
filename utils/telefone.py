"""
Módulo compartilhado para validação e verificação de operadora de telefone.
Usado pelos scripts 3 (consulta-operadora) e 4 (separador_operadora).
"""
import phonenumbers
from phonenumbers import carrier
from phonenumbers.phonenumberutil import number_type, PhoneNumberType
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def verificar_operadora(numero) -> str:
    """
    Verifica a operadora de um número de telefone brasileiro.

    Corrige automaticamente celulares com 10 dígitos (faltando o dígito 9).

    Returns:
        Nome da operadora em maiúsculas, "Linha Fixa", ou string de erro.
        String vazia para valores nulos/vazios.
    """
    if pd.isna(numero) or str(numero).strip() == "":
        return ""

    numero_str = str(numero).strip()

    if numero_str.startswith('+'):
        numero_completo = numero_str
        numero_limpo = ''.join(filter(str.isdigit, numero_str[1:]))
    else:
        numero_limpo = ''.join(filter(str.isdigit, numero_str))
        numero_completo = "+55" + numero_limpo

    if len(numero_limpo) < 10:
        return "Inválido (Curto)"

    parsed_number = None

    try:
        parsed_number = phonenumbers.parse(numero_completo, "BR")

        if not phonenumbers.is_valid_number(parsed_number):
            # Tenta corrigir celular de 10 dígitos adicionando o '9'
            if len(numero_limpo) == 10 and numero_limpo[2] != '9':
                corrigido = numero_limpo[:2] + '9' + numero_limpo[2:]
                try:
                    parsed_corrigido = phonenumbers.parse("+55" + corrigido, "BR")
                    if phonenumbers.is_valid_number(parsed_corrigido):
                        parsed_number = parsed_corrigido
                    else:
                        return "Inválido (Corrigido Falhou)"
                except Exception:
                    return "Inválido (Corrigido Falhou)"
            else:
                return "Inválido (Formato)"

    except phonenumbers.NumberParseException as e:
        logger.debug(f"Erro de parsing para {numero_str}: {e}")
        return "Inválido (Erro Parsing)"
    except Exception as e:
        logger.error(f"Erro inesperado ao processar {numero_str}: {e}", exc_info=True)
        return "Inválido (Erro Parsing)"

    if parsed_number is None:
        return "Inválido (Erro Interno)"

    try:
        operadora = carrier.name_for_number(parsed_number, "pt")
        if not operadora:
            n_type = number_type(parsed_number)
            if n_type == PhoneNumberType.FIXED_LINE:
                return "Linha Fixa"
            elif n_type == PhoneNumberType.MOBILE:
                return "Celular (Operadora não identificada)"
            return "Operadora Não Encontrada"
        return operadora.upper()
    except Exception as e:
        logger.error(f"Erro ao identificar operadora para {numero_str}: {e}", exc_info=True)
        return "Operadora Não Encontrada"
