"""Construtor da gold de streaming em micro-lotes (Cloud Run Job ou Cloud Shell).

Por que micro-lote e não mensagem a mensagem? A linha da gold só existe quando as cinco
fontes de uma escola chegaram, e a parte de docentes é uma agregação de dezenas de linhas.
Isso é processamento com estado. Em vez de manter estado num serviço, cada ciclo:

  1. executa um MERGE que lê as views de consolidação (deduplicação + agregação, as mesmas
     regras da fase 1), seleciona as escolas completas e estáveis (nada novo chegou nos
     últimos --atraso segundos, uma marca d'água simples) e insere ou atualiza a gold;
     uma escola que recebe um dado tardio ganha uma nova versão (versao + 1);
  2. opcionalmente, roda o modelo em produção (BigQuery ML) nas versões ainda não preditas
     e grava em predicoes_ideb_stream.

Uso:
  python construir_gold.py --projeto P --dataset trabalho_pdm                 # um ciclo
  python construir_gold.py --projeto P --loop 30 --duracao 3600              # ciclo a cada 30 s por 1 h
  python construir_gold.py --projeto P --modelo modelo_ideb --loop 30        # com predição
  python construir_gold.py --projeto P --mostrar-sql                         # só imprime o SQL
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

import sql_gold as S


def _log(msg: str, **campos) -> None:
    print(json.dumps({"severity": "INFO", "message": msg, **campos}, ensure_ascii=False, default=str), flush=True)


def ciclo(cli, ref, atraso: int, modelo: str | None) -> dict:
    t0 = time.time()
    job = cli.query(S.merge_gold(ref, atraso))
    job.result()
    st = job.dml_stats
    res = {"gold_inseridas": st.inserted_row_count if st else job.num_dml_affected_rows,
           "gold_atualizadas": st.updated_row_count if st else None,
           "bytes_processados": job.total_bytes_processed}
    if modelo:
        jp = cli.query(S.merge_predicoes(ref, modelo))
        jp.result()
        sp = jp.dml_stats
        res["predicoes_novas"] = sp.inserted_row_count if sp else jp.num_dml_affected_rows
        res["predicoes_atualizadas"] = sp.updated_row_count if sp else None
    res["segundos"] = round(time.time() - t0, 1)
    return res


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--projeto", default=os.environ.get("PROJETO"))
    ap.add_argument("--dataset", default=os.environ.get("DATASET", "trabalho_pdm"))
    ap.add_argument("--atraso", type=int, default=int(os.environ.get("ATRASO_SEGUNDOS", "20")),
                    help="segundos sem novidade para considerar a escola estável (marca d'água)")
    ap.add_argument("--modelo", default=os.environ.get("MODELO", ""),
                    help="nome do modelo BigQuery ML para predizer (vazio = sem predição)")
    ap.add_argument("--loop", type=int, default=int(os.environ.get("LOOP_SEGUNDOS", "0")),
                    help="intervalo entre ciclos; 0 = um único ciclo")
    ap.add_argument("--duracao", type=int, default=int(os.environ.get("DURACAO_SEGUNDOS", "3600")),
                    help="tempo máximo em modo loop")
    ap.add_argument("--mostrar-sql", action="store_true")
    args = ap.parse_args()

    if not args.projeto:
        ap.error("informe --projeto ou a variável PROJETO")
    ref = S.ref_bigquery(args.projeto, args.dataset)
    if args.mostrar_sql:
        print(S.merge_gold(ref, args.atraso), ";\n")
        if args.modelo:
            print(S.merge_predicoes(ref, args.modelo), ";")
        return

    from google.cloud import bigquery
    cli = bigquery.Client(project=args.projeto)
    fim = time.time() + args.duracao
    n = 0
    while True:
        n += 1
        try:
            res = ciclo(cli, ref, args.atraso, args.modelo or None)
            _log(f"ciclo {n} concluido", ciclo=n, **res, em=datetime.now(timezone.utc))
        except Exception as exc:  # um ciclo com erro não derruba o loop
            _log(f"ciclo {n} falhou", ciclo=n, erro=str(exc))
            if not args.loop:
                raise
        if not args.loop or time.time() + args.loop > fim:
            break
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
