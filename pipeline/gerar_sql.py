"""Grava na pasta sql/ o DDL das tabelas de streaming, as views e os MERGEs, gerados a partir
de esquema.py. Rode de novo se mudar o projeto, o dataset ou o nome do modelo.

  python pipeline/gerar_sql.py --projeto platinum-avenue-505512-t5 --dataset trabalho_pdm --modelo modelo_ideb
"""
from __future__ import annotations

import argparse
from pathlib import Path

import sql_gold as S

CABECALHO = "-- Arquivo gerado por pipeline/gerar_sql.py a partir de pipeline/esquema.py. Não edite à mão.\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--projeto", default="platinum-avenue-505512-t5")
    ap.add_argument("--dataset", default="trabalho_pdm")
    ap.add_argument("--modelo", default="modelo_ideb")
    ap.add_argument("--atraso", type=int, default=20)
    ap.add_argument("--saida", default=str(Path(__file__).resolve().parents[1] / "sql"))
    a = ap.parse_args()
    ref = S.ref_bigquery(a.projeto, a.dataset)
    out = Path(a.saida)
    out.mkdir(parents=True, exist_ok=True)
    (out / "01_tabelas_stream.sql").write_text(
        CABECALHO + "-- Tabelas de streaming: bronze crua, 5 silvers, auditoria, gold e predições.\n\n"
        + "\n\n".join(S.ddl_tabelas(ref)) + "\n", encoding="utf-8")
    (out / "02_views.sql").write_text(
        CABECALHO + "-- Views de consolidação (deduplicação e agregação da fase 1) e view unificada lote + streaming.\n\n"
        + "\n\n".join(S.ddl_views(ref)) + "\n", encoding="utf-8")
    (out / "05_merge_gold.sql").write_text(
        CABECALHO + f"-- O mesmo MERGE que construir_gold.py executa a cada ciclo (marca d'água de {a.atraso} s).\n\n"
        + S.merge_gold(ref, a.atraso) + ";\n", encoding="utf-8")
    (out / "06_merge_predicoes.sql").write_text(
        CABECALHO + "-- Predição com o modelo em produção para as versões da gold ainda não preditas.\n\n"
        + S.merge_predicoes(ref, a.modelo) + ";\n", encoding="utf-8")
    print("SQL gravado em", out)


if __name__ == "__main__":
    main()
