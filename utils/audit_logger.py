"""
Log de auditoria estruturado em JSON para o pipeline AutomacaoBandaLarga.

Cada evento de auditoria é gravado como uma linha JSON (JSON Lines / NDJSON)
em um arquivo de auditoria centralizado, separado dos logs operacionais.

Campos obrigatórios em todo evento:
  - timestamp   : ISO 8601
  - script      : nome do script que gerou o evento
  - evento      : tipo do evento (inicio, conclusao, erro, aviso)
  - mensagem    : descrição legível

Campos opcionais (incluídos quando disponíveis):
  - arquivo     : nome do arquivo sendo processado
  - total       : total de registros no arquivo
  - processados : registros processados com sucesso
  - falhas      : registros com erro
  - duracao_s   : duração em segundos (float)
  - usuario     : usuário do SO que executou o script
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).parent.parent
AUDIT_LOG_FILE = _ROOT / "audit.jsonl"

_audit_logger = logging.getLogger("auditoria")


def _gravar(evento: dict) -> None:
    """Grava uma linha JSON no arquivo de auditoria."""
    linha = json.dumps(evento, ensure_ascii=False)
    try:
        with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(linha + "\n")
    except Exception as e:
        _audit_logger.error(f"Falha ao gravar auditoria: {e} | evento={linha}")


def _base(script: str, evento: str, mensagem: str, **kwargs) -> dict:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "script": script,
        "evento": evento,
        "mensagem": mensagem,
        "usuario": os.getenv("USERNAME") or os.getenv("USER") or "desconhecido",
    }
    entry.update({k: v for k, v in kwargs.items() if v is not None})
    return entry


def registrar_inicio(script: str, mensagem: str = "Execução iniciada", **kwargs) -> None:
    _gravar(_base(script, "inicio", mensagem, **kwargs))


def registrar_conclusao(
    script: str,
    arquivo: str | None = None,
    total: int | None = None,
    processados: int | None = None,
    falhas: int | None = None,
    duracao_s: float | None = None,
    mensagem: str = "Execução concluída",
    **kwargs,
) -> None:
    _gravar(_base(
        script, "conclusao", mensagem,
        arquivo=arquivo,
        total=total,
        processados=processados,
        falhas=falhas,
        duracao_s=round(duracao_s, 3) if duracao_s is not None else None,
        **kwargs,
    ))


def registrar_erro(script: str, mensagem: str, arquivo: str | None = None, **kwargs) -> None:
    _gravar(_base(script, "erro", mensagem, arquivo=arquivo, **kwargs))


def registrar_aviso(script: str, mensagem: str, arquivo: str | None = None, **kwargs) -> None:
    _gravar(_base(script, "aviso", mensagem, arquivo=arquivo, **kwargs))


class AuditTimer:
    """Context manager que mede duração e registra conclusão automaticamente."""

    def __init__(self, script: str, arquivo: str | None = None):
        self.script = script
        self.arquivo = arquivo
        self._start: float = 0.0
        self.total: int | None = None
        self.processados: int | None = None
        self.falhas: int | None = None

    def __enter__(self) -> "AuditTimer":
        self._start = time.monotonic()
        registrar_inicio(self.script, arquivo=self.arquivo)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duracao = time.monotonic() - self._start
        if exc_type:
            registrar_erro(
                self.script,
                mensagem=f"Execução encerrada com exceção: {exc_val}",
                arquivo=self.arquivo,
                duracao_s=duracao,
            )
        else:
            registrar_conclusao(
                self.script,
                arquivo=self.arquivo,
                total=self.total,
                processados=self.processados,
                falhas=self.falhas,
                duracao_s=duracao,
            )
        return False  # não suprime exceções
