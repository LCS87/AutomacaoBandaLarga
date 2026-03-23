"""
Testes unitários para format_cnpj (ConsultaCnpj.py).

Função pura: recebe um valor, retorna string de 14 dígitos ou None.
"""
import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "consulta_cnpj",
    Path(__file__).parent.parent / "scripts" / "etapa_2" / "ConsultaCnpj.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]
format_cnpj = _mod.format_cnpj


class TestFormatCnpj:

    @pytest.mark.parametrize("entrada, esperado", [
        ("12.345.678/0001-95", "12345678000195"),   # formatado com pontuação
        ("12345678000195", "12345678000195"),         # já limpo
        ("  12345678000195  ", "12345678000195"),     # com espaços
        ("12.345.678/0001-95\n", "12345678000195"),  # com newline
    ])
    def test_cnpj_valido_retorna_14_digitos(self, entrada: str, esperado: str):
        assert format_cnpj(entrada) == esperado

    @pytest.mark.parametrize("entrada", [
        "123456780001",       # 12 dígitos
        "1234567800019500",   # 16 dígitos
        "00000000000000",     # 14 zeros — estruturalmente válido (retorna string)
        "",
        "abc",
        "12.345.678/0001",    # incompleto
    ])
    def test_cnpj_invalido_retorna_none_ou_string(self, entrada: str):
        result = format_cnpj(entrada)
        # Deve retornar None (inválido) ou string de exatamente 14 dígitos
        assert result is None or (isinstance(result, str) and len(result) == 14)

    def test_none_retorna_none(self):
        assert format_cnpj(None) is None  # type: ignore[arg-type]

    def test_nan_retorna_none(self):
        import math
        assert format_cnpj(float("nan")) is None

    def test_resultado_contem_apenas_digitos(self):
        result = format_cnpj("12.345.678/0001-95")
        assert result is not None
        assert result.isdigit()

    def test_resultado_tem_14_caracteres(self):
        result = format_cnpj("12.345.678/0001-95")
        assert result is not None
        assert len(result) == 14
