"""
Testes unitários para domain/empresa.py — dataclass Empresa.

Valida: construção, valores padrão, to_dict e ausência de campos de infraestrutura.
"""
import pytest
from domain.empresa import Empresa


class TestEmpresaConstrucao:

    def test_campos_obrigatorios(self):
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        assert e.cnpj == "12345678000195"
        assert e.razao_social == "TESTE LTDA"

    def test_campos_opcionais_tem_valor_padrao_vazio(self):
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        assert e.nome_fantasia == ""
        assert e.situacao_cadastral == ""
        assert e.data_abertura == ""
        assert e.natureza_juridica == ""
        assert e.capital_social == ""
        assert e.atividade_principal == ""
        assert e.cep == ""
        assert e.logradouro == ""
        assert e.numero == ""
        assert e.complemento == ""
        assert e.bairro == ""
        assert e.municipio == ""
        assert e.uf == ""
        assert e.endereco_completo == ""
        assert e.telefone == ""
        assert e.email == ""


class TestEmpresaToDict:

    def test_to_dict_retorna_dict(self):
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        assert isinstance(e.to_dict(), dict)

    def test_to_dict_contem_chaves_do_pipeline(self):
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        d = e.to_dict()
        chaves_esperadas = {
            "CNPJ", "RazaoSocial", "NomeFantasia", "SituacaoCadastral",
            "DataAbertura", "NaturezaJuridica", "CapitalSocial", "AtividadePrincipal",
            "CEP", "Logradouro", "Numero", "Complemento", "Bairro",
            "Municipio", "UF", "EnderecoCompleto", "Telefone", "Email",
        }
        assert chaves_esperadas == set(d.keys())

    def test_to_dict_nao_contem_campos_de_infraestrutura(self):
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        d = e.to_dict()
        campos_infra = {"_cached_at", "Fonte", "cached_at", "fonte"}
        assert campos_infra.isdisjoint(set(d.keys()))

    def test_to_dict_valores_corretos(self):
        e = Empresa(
            cnpj="12345678000195",
            razao_social="EMPRESA TESTE LTDA",
            municipio="Campinas",
            uf="SP",
            email="contato@empresa.com",
        )
        d = e.to_dict()
        assert d["CNPJ"] == "12345678000195"
        assert d["RazaoSocial"] == "EMPRESA TESTE LTDA"
        assert d["Municipio"] == "Campinas"
        assert d["UF"] == "SP"
        assert d["Email"] == "contato@empresa.com"

    def test_to_dict_e_serializavel_em_json(self):
        import json
        e = Empresa(cnpj="12345678000195", razao_social="TESTE LTDA")
        # Não deve lançar exceção
        json.dumps(e.to_dict())
