"""
Cliente HTTP para a API de consulta de CNPJ.

Responsabilidades:
- Executar requisições HTTP com retry e rate limiting
- Fazer o parsing da resposta da API para o modelo de domínio Empresa
- NÃO conhece cache, NÃO conhece planilhas, NÃO conhece o pipeline
"""
import logging
import time

import requests

from domain.empresa import Empresa
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


def _first_value(data: dict, keys: list) -> str:
    for k in keys:
        if v := data.get(k):
            return str(v)
    return ""


def _parse_response(data: dict) -> Empresa:
    """Converte a resposta bruta da API em um objeto Empresa."""
    endereco = data.get("endereco", {}) or {}

    logradouro   = endereco.get("logradouro", "") or ""
    numero       = endereco.get("numero", "") or ""
    complemento  = endereco.get("complemento", "") or ""
    bairro       = endereco.get("bairro", "") or ""
    municipio    = endereco.get("municipio", "") or ""
    uf           = endereco.get("uf", "") or ""
    cep          = endereco.get("cep", "") or ""

    endereco_completo = ", ".join(filter(None, [
        logradouro, numero, complemento, bairro, municipio, uf, cep
    ]))

    telefones = []
    for campo in ("telefone1", "telefone2"):
        if t := data.get(campo):
            telefones.append(str(t).strip())
    if t := endereco.get("telefone"):
        telefones.append(str(t).strip())
    telefone = " / ".join(sorted(set(telefones))) if telefones else ""

    atividade = ""
    if isinstance(data.get("atividade_principal"), dict):
        atividade = data["atividade_principal"].get("descricao", "") or ""

    return Empresa(
        cnpj=_first_value(data, ["cnpj"]),
        razao_social=_first_value(data, ["razao_social"]) or "NÃO ENCONTRADO",
        nome_fantasia=_first_value(data, ["nome_fantasia"]),
        situacao_cadastral=(data.get("situacao") or {}).get("nome", "") or "",
        data_abertura=_first_value(data, ["data_inicio"]),
        natureza_juridica=_first_value(data, ["natureza_juridica"]),
        capital_social=_first_value(data, ["capital_social"]),
        atividade_principal=atividade,
        cep=cep,
        logradouro=logradouro,
        numero=numero,
        complemento=complemento,
        bairro=bairro,
        municipio=municipio,
        uf=uf,
        endereco_completo=endereco_completo,
        telefone=telefone,
        email=data.get("email") or "",
    )


class CnpjApiClient:
    def __init__(self, api_key: str, api_url: str, rate_limiter: RateLimiter, timeout: int = 10):
        self._api_key = api_key
        self._api_url = api_url.rstrip("/")
        self._rate_limiter = rate_limiter
        self._timeout = timeout

    def consultar(self, cnpj: str) -> Empresa | None:
        """
        Consulta um CNPJ na API com até 3 tentativas e rate limiting.
        Retorna None se todas as tentativas falharem.
        """
        url = f"{self._api_url}/{cnpj}"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for tentativa in range(3):
            self._rate_limiter.wait_if_needed()
            try:
                r = requests.get(url, headers=headers, timeout=self._timeout)
                if r.status_code == 200:
                    return _parse_response(r.json())
                elif r.status_code == 429:
                    wait = 60 * (tentativa + 1)
                    logger.warning(f"Rate limit da API (429). Aguardando {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Erro {r.status_code} ao consultar {cnpj}: {r.text[:200]}")
                    return None
            except requests.Timeout:
                logger.warning(f"Timeout na tentativa {tentativa + 1} para {cnpj}")
            except Exception as e:
                logger.error(f"Erro de conexão ao consultar {cnpj}: {e}")
                return None

        return None
