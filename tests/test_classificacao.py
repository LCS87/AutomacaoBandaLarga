"""
Testes unitários para config/classificacao.py e a função classificar_cliente
do separador.py.

classificar_cliente é uma função pura: sem I/O, sem estado, sem dependências
externas — ideal para testes exaustivos.
"""
import pytest
from config.classificacao import MEI_APROVADOS, DEMAIS_APROVADOS


# ── Importa a função diretamente do módulo do script ─────────────────────────
# Usamos importlib para evitar efeitos colaterais do módulo (mkdir, logging)
import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "separador",
    Path(__file__).parent.parent / "scripts" / "etapa_1" / "separador.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]
classificar_cliente = _mod.classificar_cliente


# ── Testes dos frozensets ─────────────────────────────────────────────────────

class TestFrozensets:
    def test_mei_aprovados_e_imutavel(self):
        assert isinstance(MEI_APROVADOS, frozenset)

    def test_demais_aprovados_e_imutavel(self):
        assert isinstance(DEMAIS_APROVADOS, frozenset)

    def test_sem_intersecao_entre_categorias(self):
        """Nenhuma descrição pode pertencer a MEI e DEMAIS ao mesmo tempo."""
        assert MEI_APROVADOS.isdisjoint(DEMAIS_APROVADOS)

    def test_mei_nao_vazio(self):
        assert len(MEI_APROVADOS) > 0

    def test_demais_nao_vazio(self):
        assert len(DEMAIS_APROVADOS) > 0


# ── Testes de classificar_cliente ─────────────────────────────────────────────

class TestClassificarCliente:

    @pytest.mark.parametrize("descricao", sorted(MEI_APROVADOS))
    def test_todas_descricoes_mei_retornam_mei(self, descricao: str):
        assert classificar_cliente(descricao) == "MEI"

    @pytest.mark.parametrize("descricao", sorted(DEMAIS_APROVADOS))
    def test_todas_descricoes_demais_retornam_demais(self, descricao: str):
        assert classificar_cliente(descricao) == "DEMAIS"

    @pytest.mark.parametrize("entrada", [
        "Descrição desconhecida",
        "cliente mei com alta probabilidade",   # case-sensitive
        "Cliente MEI com alta probabilidade de aprovacao",  # sem acento
        "",
        "   ",
        "ANALISE_CREDITO=Cliente MEI com alta probabilidade de aprovação",  # com prefixo
    ])
    def test_descricoes_invalidas_retornam_descartar(self, entrada: str):
        assert classificar_cliente(entrada) == "DESCARTAR"

    @pytest.mark.parametrize("entrada", [None, 123, 3.14, [], {}])
    def test_tipos_nao_string_retornam_descartar(self, entrada):
        assert classificar_cliente(entrada) == "DESCARTAR"
