"""
Cache persistente de consultas de CNPJ.

Responsabilidades:
- Armazenar e recuperar objetos Empresa com TTL configurável
- Persistir em JSON e carregar do disco
- Controlar dirty flag para flush eficiente
- NÃO conhece HTTP, NÃO conhece planilhas
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from domain.empresa import Empresa

logger = logging.getLogger(__name__)


@dataclass
class _CacheEntry:
    empresa: Empresa
    cached_at: datetime
    fonte: str = "API"

    def to_dict(self) -> dict:
        d = self.empresa.to_dict()
        d["_cached_at"] = self.cached_at.isoformat()
        d["Fonte"] = self.fonte
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "_CacheEntry":
        cached_at_str = data.pop("_cached_at", None)
        fonte = data.pop("Fonte", "API")
        cached_at = datetime.fromisoformat(cached_at_str) if cached_at_str else datetime.min

        empresa = Empresa(
            cnpj=data.get("CNPJ", ""),
            razao_social=data.get("RazaoSocial", ""),
            nome_fantasia=data.get("NomeFantasia", ""),
            situacao_cadastral=data.get("SituacaoCadastral", ""),
            data_abertura=data.get("DataAbertura", ""),
            natureza_juridica=data.get("NaturezaJuridica", ""),
            capital_social=data.get("CapitalSocial", ""),
            atividade_principal=data.get("AtividadePrincipal", ""),
            cep=data.get("CEP", ""),
            logradouro=data.get("Logradouro", ""),
            numero=data.get("Numero", ""),
            complemento=data.get("Complemento", ""),
            bairro=data.get("Bairro", ""),
            municipio=data.get("Municipio", ""),
            uf=data.get("UF", ""),
            endereco_completo=data.get("EnderecoCompleto", ""),
            telefone=data.get("Telefone", ""),
            email=data.get("Email", ""),
        )
        return cls(empresa=empresa, cached_at=cached_at, fonte=fonte)


class CnpjCache:
    def __init__(self, cache_file: Path, ttl_days: int = 30):
        self._cache_file = cache_file
        self._ttl = timedelta(days=ttl_days)
        self._store: dict[str, _CacheEntry] = {}
        self._dirty = False

    def load(self) -> None:
        """Carrega o cache do disco. Entradas corrompidas são ignoradas."""
        if not self._cache_file.exists():
            return
        try:
            with open(self._cache_file, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
            for cnpj, entry_data in raw.items():
                try:
                    self._store[cnpj] = _CacheEntry.from_dict(dict(entry_data))
                except Exception as e:
                    logger.warning(f"Entrada de cache inválida para {cnpj}: {e}. Ignorando.")
            logger.info(f"Cache carregado: {len(self._store)} entradas de {self._cache_file.name}")
        except Exception as e:
            logger.warning(f"Erro ao carregar cache: {e}. Iniciando vazio.")
            self._store = {}

    def flush(self) -> None:
        """Persiste o cache no disco apenas se houver mudanças."""
        if not self._dirty:
            return
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_file, "w", encoding="utf-8") as f:
                json.dump(
                    {cnpj: entry.to_dict() for cnpj, entry in self._store.items()},
                    f, indent=2, ensure_ascii=False
                )
            self._dirty = False
        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")

    def get(self, cnpj: str) -> Empresa | None:
        """Retorna Empresa se existir no cache e não estiver expirada."""
        entry = self._store.get(cnpj)
        if entry is None:
            return None
        if datetime.now() - entry.cached_at >= self._ttl:
            return None
        if entry.empresa.razao_social == "NÃO ENCONTRADO":
            return None
        return entry.empresa

    def set(self, cnpj: str, empresa: Empresa, fonte: str = "API") -> None:
        """Armazena uma Empresa no cache e marca como dirty."""
        self._store[cnpj] = _CacheEntry(empresa=empresa, cached_at=datetime.now(), fonte=fonte)
        self._dirty = True

    def __len__(self) -> int:
        return len(self._store)
