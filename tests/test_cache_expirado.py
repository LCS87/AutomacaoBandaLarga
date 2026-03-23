"""
Testes unitários para a lógica de expiração de cache em infra/cnpj_cache.py.

Testa _CacheEntry diretamente e o comportamento de get() com TTL variado.
"""
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from domain.empresa import Empresa
from infra.cnpj_cache import CnpjCache, _CacheEntry


def _empresa() -> Empresa:
    return Empresa(cnpj="12345678000195", razao_social="EMPRESA TESTE LTDA")


class TestCacheEntryToDict:

    def test_to_dict_inclui_cached_at(self):
        entry = _CacheEntry(empresa=_empresa(), cached_at=datetime(2025, 1, 15, 12, 0, 0))
        d = entry.to_dict()
        assert "_cached_at" in d
        assert "2025-01-15" in d["_cached_at"]

    def test_to_dict_inclui_fonte(self):
        entry = _CacheEntry(empresa=_empresa(), cached_at=datetime.now(), fonte="API")
        d = entry.to_dict()
        assert d["Fonte"] == "API"

    def test_from_dict_reconstroi_entry(self):
        original = _CacheEntry(empresa=_empresa(), cached_at=datetime(2025, 3, 1, 8, 0, 0))
        d = original.to_dict()
        reconstruido = _CacheEntry.from_dict(d)
        assert reconstruido.empresa.cnpj == original.empresa.cnpj
        assert reconstruido.empresa.razao_social == original.empresa.razao_social
        assert reconstruido.cached_at.date() == original.cached_at.date()

    def test_from_dict_sem_cached_at_usa_datetime_min(self):
        d = {"CNPJ": "12345678000195", "RazaoSocial": "TESTE", "Fonte": "API"}
        entry = _CacheEntry.from_dict(d)
        assert entry.cached_at == datetime.min


class TestCacheExpiracaoPorTTL:

    @pytest.mark.parametrize("ttl_days, dias_atras, deve_expirar", [
        (30, 29, False),   # dentro do TTL
        (30, 30, True),    # exatamente no limite — expirado
        (30, 31, True),    # além do TTL
        (1,  0,  False),   # TTL de 1 dia, entrada de hoje
        (1,  2,  True),    # TTL de 1 dia, entrada de 2 dias atrás
        (90, 89, False),   # TTL longo, dentro
        (90, 91, True),    # TTL longo, fora
    ])
    def test_expiracao_por_ttl(
        self, tmp_path: Path, ttl_days: int, dias_atras: int, deve_expirar: bool
    ):
        cache = CnpjCache(tmp_path / "cache.json", ttl_days=ttl_days)
        empresa = _empresa()
        cache.set(empresa.cnpj, empresa)

        # Retroage o cached_at
        entry = cache._store[empresa.cnpj]
        entry.cached_at = datetime.now() - timedelta(days=dias_atras)

        resultado = cache.get(empresa.cnpj)
        if deve_expirar:
            assert resultado is None, f"TTL={ttl_days}, dias_atras={dias_atras}: deveria expirar"
        else:
            assert resultado is not None, f"TTL={ttl_days}, dias_atras={dias_atras}: não deveria expirar"

    def test_ttl_zero_expira_imediatamente(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json", ttl_days=0)
        empresa = _empresa()
        cache.set(empresa.cnpj, empresa)
        # Retroage 1 segundo para garantir que a diferença >= timedelta(0)
        cache._store[empresa.cnpj].cached_at = datetime.now() - timedelta(seconds=1)
        assert cache.get(empresa.cnpj) is None
