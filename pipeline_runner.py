"""
Orquestrador do pipeline AutomacaoBandaLarga.

Executa as 4 etapas em sequência e exibe um relatório consolidado ao final.
Cada etapa é um subprocesso isolado — falhas em uma etapa não bloqueiam as demais
(comportamento configurável via --fail-fast).

Uso:
    python pipeline_runner.py
    python pipeline_runner.py --dry-run
    python pipeline_runner.py --etapas 1 2
    python pipeline_runner.py --etapas 3 4 --fail-fast
"""
import argparse
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.audit_logger import registrar_inicio, registrar_conclusao, registrar_erro

SCRIPT_NAME = "pipeline-runner"

ETAPAS = {
    1: {
        "nome": "Separação de Prospects",
        "script": _ROOT / "scripts" / "etapa_1" / "separador.py",
    },
    2: {
        "nome": "Consulta de CNPJ",
        "script": _ROOT / "scripts" / "etapa_2" / "ConsultaCnpj.py",
    },
    3: {
        "nome": "Consulta de Operadora",
        "script": _ROOT / "scripts" / "etapa_3" / "consulta_operadora.py",
    },
    4: {
        "nome": "Arquivo Discadora",
        "script": _ROOT / "scripts" / "etapa_4" / "separador_operadora.py",
    },
}


@dataclass
class ResultadoEtapa:
    numero: int
    nome: str
    sucesso: bool
    duracao_s: float
    codigo_saida: int
    erro: str = ""


def _executar_etapa(numero: int, nome: str, script: Path, dry_run: bool) -> ResultadoEtapa:
    cmd = [sys.executable, str(script)]
    if dry_run:
        cmd.append("--dry-run")

    print(f"\n{'='*60}")
    print(f"  Etapa {numero}: {nome}")
    print(f"{'='*60}")

    inicio = time.monotonic()
    try:
        result = subprocess.run(cmd, check=False)
        duracao = time.monotonic() - inicio
        sucesso = result.returncode == 0
        return ResultadoEtapa(
            numero=numero,
            nome=nome,
            sucesso=sucesso,
            duracao_s=duracao,
            codigo_saida=result.returncode,
            erro="" if sucesso else f"Código de saída: {result.returncode}",
        )
    except Exception as e:
        duracao = time.monotonic() - inicio
        return ResultadoEtapa(
            numero=numero,
            nome=nome,
            sucesso=False,
            duracao_s=duracao,
            codigo_saida=-1,
            erro=str(e),
        )


def _imprimir_relatorio(resultados: list[ResultadoEtapa], duracao_total: float) -> None:
    print(f"\n{'='*60}")
    print("  RELATÓRIO DO PIPELINE")
    print(f"{'='*60}")
    print(f"  {'Etapa':<5} {'Nome':<30} {'Status':<10} {'Duração':>10}")
    print(f"  {'-'*55}")
    for r in resultados:
        status = "OK" if r.sucesso else "FALHOU"
        print(f"  {r.numero:<5} {r.nome:<30} {status:<10} {r.duracao_s:>8.2f}s")
        if r.erro:
            print(f"         └─ {r.erro}")
    print(f"  {'-'*55}")
    total_ok = sum(1 for r in resultados if r.sucesso)
    print(f"  Total: {total_ok}/{len(resultados)} etapas concluídas em {duracao_total:.2f}s")
    print(f"{'='*60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestrador do pipeline AutomacaoBandaLarga.")
    parser.add_argument(
        "--etapas", nargs="+", type=int, choices=[1, 2, 3, 4],
        default=[1, 2, 3, 4],
        help="Etapas a executar (padrão: todas). Ex: --etapas 1 2",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Passa --dry-run para cada etapa: valida inputs sem processar.",
    )
    parser.add_argument(
        "--fail-fast", action="store_true",
        help="Interrompe o pipeline na primeira etapa com falha.",
    )
    args = parser.parse_args()

    etapas_selecionadas = sorted(set(args.etapas))
    modo = "[DRY-RUN] " if args.dry_run else ""
    print(f"\n{modo}Iniciando pipeline — etapas: {etapas_selecionadas}")

    registrar_inicio(SCRIPT_NAME, mensagem=f"Pipeline iniciado | etapas={etapas_selecionadas} | dry_run={args.dry_run}")

    resultados: list[ResultadoEtapa] = []
    inicio_total = time.monotonic()

    for numero in etapas_selecionadas:
        etapa = ETAPAS[numero]
        resultado = _executar_etapa(numero, etapa["nome"], etapa["script"], args.dry_run)
        resultados.append(resultado)

        if resultado.sucesso:
            registrar_conclusao(
                SCRIPT_NAME,
                mensagem=f"Etapa {numero} concluída",
                duracao_s=resultado.duracao_s,
            )
        else:
            registrar_erro(
                SCRIPT_NAME,
                mensagem=f"Etapa {numero} falhou: {resultado.erro}",
            )
            if args.fail_fast:
                print(f"\n[FAIL-FAST] Etapa {numero} falhou. Interrompendo pipeline.")
                break

    duracao_total = time.monotonic() - inicio_total
    _imprimir_relatorio(resultados, duracao_total)

    total_ok = sum(1 for r in resultados if r.sucesso)
    registrar_conclusao(
        SCRIPT_NAME,
        mensagem="Pipeline finalizado",
        total=len(resultados),
        processados=total_ok,
        falhas=len(resultados) - total_ok,
        duracao_s=duracao_total,
    )

    sys.exit(0 if all(r.sucesso for r in resultados) else 1)


if __name__ == "__main__":
    main()
