"""
Regras de classificação de prospects por análise de crédito.

Centraliza os critérios de elegibilidade em frozensets imutáveis,
eliminando magic strings espalhadas pelo código de processamento.
Para adicionar ou remover categorias, edite apenas este arquivo.
"""

MEI_APROVADOS: frozenset[str] = frozenset({
    "Cliente MEI com alta probabilidade de aprovação",
    "Cliente MEI com média probabilidade de aprovação",
    "Cliente MEI com altissíma probabilidade de aprovação",
})

DEMAIS_APROVADOS: frozenset[str] = frozenset({
    "Cliente com altissíma probabilidade de aprovação",
    "Cliente com média probabilidade de aprovação",
    "Cliente aprovado até R$ 1700.00",
    "Cliente aprovado até R$ 1500.00",
    "Cliente aprovado até R$ 3000.00",
    "Cliente aprovado até R$ 4000.00",
    "Cliente aprovado até R$ 5000.00",
    "Cliente aprovado com mais de R$ 5000,00",
})
