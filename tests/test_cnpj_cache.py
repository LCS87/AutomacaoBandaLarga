"""
Testes unitários para infra/cnpj_cache.py — CnpjCache.

Testa: get/set com TTL, dirty flag, flush, load, entradas corrompidas.
Usa tmp_path do pytest para I/O real sem poluir o projeto.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from domain.empresa import Empresa
from infra.cnpj_cache import CnpjCache


def _empresa(cnpj: str = "12345678000195", razao: str = "EMPRESA TESTE LTDA") -> Empresa:
    return Empresa(cnpj=cnpj, razao_social=razao, municipio="São Paulo", uf="SP")


class TestCnpjCacheGetSet:

    def test_get_retorna_none_para_cnpj_inexistente(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        assert cache.get("00000000000000") is None

    def test_set_e_get_retornam_mesma_empresa(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        empresa = _empresa()
        cache.set(empresa.cnpj, empresa)
        resultado = cache.get(empresa.cnpj)
        assert resultado is not None
        assert resultado.cnpj == empresa.cnpj
        assert resultado.razao_social == empresa.razao_social

    def test_get_retorna_none_para_nao_encontrado(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        falha = Empresa(cnpj="11111111000111", razao_social="NÃO ENCONTRADO")
        cache.set(falha.cnpj, falha)
        assert cache.get(falha.cnpj) is None

    def test_len_reflete_quantidade_de_entradas(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        assert len(cache) == 0
        cache.set("11111111000111", _empresa("11111111000111"))
        cache.set("22222222000122", _empresa("22222222000122"))
        assert len(cache) == 2


class TestCnpjCacheTTL:

    def test_entrada_dentro_do_ttl_e_retornada(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json", ttl_days=30)
        empresa = _empresa()
        cache.set(empresa.cnpj, empresa)
        assert cache.get(empresa.cnpj) is not None

    def test_entrada_expirada_retorna_none(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json", ttl_days=1)
        empresa = _empresa()
        cache.set(empresa.cnpj, empresa)

        # Manipula o cached_at para simular expiração
        entry = cache._store[empresa.cnpj]
        entry.cached_at = datetime.now() - timedelta(days=2)

        assert cache.get(empresa.cnpj) is None


class TestCnpjCacheDirtyFlag:

    def test_dirty_false_antes_de_qualquer_set(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        assert cache._dirty is False

    def test_dirty_true_apos_set(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        cache.set("11111111000111", _empresa())
        assert cache._dirty is True

    def test_dirty_false_apos_flush(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "cache.json")
        cache.set("11111111000111", _empresa())
        cache.flush()
        assert cache._dirty is False

    def test_flush_sem_mudancas_nao_cria_arquivo(self, tmp_path: Path):
        cache_file = tmp_path / "cache.json"
        cache = CnpjCache(cache_file)
        cache.flush()  # dirty=False, não deve criar arquivo
        assert not cache_file.exists()


class TestCnpjCachePersistencia:

    def test_flush_cria_arquivo_json(self, tmp_path: Path):
        cache_file = tmp_path / "cache.json"
        cache = CnpjCache(cache_file)
        cache.set("12345678000195", _empresa())
        cache.flush()
        assert cache_file.exists()

    def test_load_recupera_dados_persistidos(self, tmp_path: Path):
        cache_file = tmp_path / "cache.json"
        empresa = _empresa()

        cache1 = CnpjCache(cache_file)
        cache1.set(empresa.cnpj, empresa)
        cache1.flush()

        cache2 = CnpjCache(cache_file)
        cache2.load()
        resultado = cache2.get(empresa.cnpj)

        assert resultado is not None
        assert resultado.cnpj == empresa.cnpj
        assert resultado.razao_social == empresa.razao_social

    def test_load_ignora_entradas_corrompidas(self, tmp_path: Path):
        cache_file = tmp_path / "cache.json"
        # Grava JSON com uma entrada válida e uma corrompida
        cache_file.write_text(json.dumps({
            "12345678000195": {
                "CNPJ": "12345678000195",
                "RazaoSocial": "EMPRESA VALIDA",
                "_cached_at": datetime.now().isoformat(),
                "Fonte": "API",
            },
            "99999999999999": "isso_nao_e_um_dict",  # corrompido
        }), encoding="utf-8")

        cache = CnpjCache(cache_file)
        cache.load()
        # A entrada válida deve ter sido carregada
        assert cache.get("12345678000195") is not None

    def test_load_arquivo_inexistente_nao_levanta_excecao(self, tmp_path: Path):
        cache = CnpjCache(tmp_path / "nao_existe.json")
        cache.load()  # não deve lançar exceção
        assert len(cache) == 0
