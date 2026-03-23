"""
Modelo de domínio para dados de uma empresa consultada via CNPJ.

Contém apenas campos de negócio — sem metadados de infraestrutura
como timestamps de cache, fonte da consulta ou flags internos.
"""
from dataclasses import dataclass, field


@dataclass
class Empresa:
    cnpj: str
    razao_social: str
    nome_fantasia: str = ""
    situacao_cadastral: str = ""
    data_abertura: str = ""
    natureza_juridica: str = ""
    capital_social: str = ""
    atividade_principal: str = ""
    # Endereço
    cep: str = ""
    logradouro: str = ""
    numero: str = ""
    complemento: str = ""
    bairro: str = ""
    municipio: str = ""
    uf: str = ""
    endereco_completo: str = ""
    # Contato
    telefone: str = ""
    email: str = ""

    def to_dict(self) -> dict:
        """Serializa para dict com as chaves no formato esperado pelo pipeline."""
        return {
            "CNPJ": self.cnpj,
            "RazaoSocial": self.razao_social,
            "NomeFantasia": self.nome_fantasia,
            "SituacaoCadastral": self.situacao_cadastral,
            "DataAbertura": self.data_abertura,
            "NaturezaJuridica": self.natureza_juridica,
            "CapitalSocial": self.capital_social,
            "AtividadePrincipal": self.atividade_principal,
            "CEP": self.cep,
            "Logradouro": self.logradouro,
            "Numero": self.numero,
            "Complemento": self.complemento,
            "Bairro": self.bairro,
            "Municipio": self.municipio,
            "UF": self.uf,
            "EnderecoCompleto": self.endereco_completo,
            "Telefone": self.telefone,
            "Email": self.email,
        }
