"""
Testes unitários para utils/telefone.py — verificar_operadora.

Função pura (sem I/O, sem estado): recebe número, retorna string de operadora.
Os testes validam o comportamento de parsing e classificação, não o banco
de dados de portabilidade (que pode mudar).
"""
import pytest
from utils.telefone import verificar_operadora


class TestVerificarOperadora:

    # ── Entradas nulas / vazias ───────────────────────────────────────────────

    def test_none_retorna_vazio(self):
        assert verificar_operadora(None) == ""  # type: ignore[arg-type]

    def test_string_vazia_retorna_vazio(self):
        assert verificar_operadora("") == ""

    def test_apenas_espacos_retorna_vazio(self):
        assert verificar_operadora("   ") == ""

    def test_nan_retorna_vazio(self):
        import math
        assert verificar_operadora(float("nan")) == ""

    # ── Números inválidos ─────────────────────────────────────────────────────

    def test_numero_muito_curto_retorna_invalido(self):
        result = verificar_operadora("123")
        assert "Inválido" in result

    def test_numero_com_letras_retorna_invalido(self):
        result = verificar_operadora("abcdefghij")
        assert "Inválido" in result or result == ""

    # ── Números válidos — verifica apenas que retorna string não vazia ────────

    def test_celular_sp_retorna_string_nao_vazia(self):
        # 11 9xxxx-xxxx — celular SP com 9 dígito
        result = verificar_operadora("11987654321")
        assert isinstance(result, str)
        assert result != ""

    def test_celular_com_ddd_e_formato_internacional(self):
        result = verificar_operadora("+5511987654321")
        assert isinstance(result, str)
        assert result != ""

    def test_numero_fixo_retorna_linha_fixa_ou_string(self):
        # Número fixo SP: 11 3xxx-xxxx
        result = verificar_operadora("1133334444")
        assert isinstance(result, str)
        # Pode ser "Linha Fixa" ou nome de operadora
        assert result != ""

    # ── Correção automática de celular com 10 dígitos ─────────────────────────

    def test_celular_10_digitos_sem_nono_digito_e_corrigido(self):
        # 11 8xxxx-xxxx (10 dígitos, sem o 9) deve ser corrigido para 11 98xxxx-xxxx
        result = verificar_operadora("1187654321")
        # Não deve retornar "Inválido (Curto)" — foi corrigido ou tentou corrigir
        assert result != "Inválido (Curto)"

    # ── Tipo de retorno ───────────────────────────────────────────────────────

    def test_retorno_e_sempre_string(self):
        for numero in ["11987654321", "abc", None, "", "00000000000"]:
            result = verificar_operadora(numero)  # type: ignore[arg-type]
            assert isinstance(result, str), f"Esperava str para entrada {numero!r}, got {type(result)}"

    # ── Resultado em maiúsculas para operadoras identificadas ─────────────────

    def test_operadora_identificada_esta_em_maiusculas(self):
        result = verificar_operadora("11987654321")
        # Se identificou operadora (não é erro/inválido/linha fixa), deve ser maiúsculo
        erros = {"Inválido", "Linha Fixa", "Celular", "Operadora Não Encontrada", ""}
        if not any(result.startswith(e) for e in erros):
            assert result == result.upper(), f"Operadora deveria estar em maiúsculas: {result!r}"
