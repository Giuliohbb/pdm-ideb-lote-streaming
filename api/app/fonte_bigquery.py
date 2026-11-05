"""Leitura de uma escola na gold (lote e streaming) para a rota GET /escolas/{id}/predicao.

A consulta usa a view vw_gold_ideb_unificada, que junta a gold histórica (lote) e a gold de
streaming, e traz junto a predição que o job da gold gravou com ML.PREDICT para as linhas de
streaming. Assim a resposta mostra, lado a lado, a previsão da API e a do BigQuery.
"""
from __future__ import annotations

from typing import Optional

from .features import NOMES

CONSULTA = """
SELECT g.origem_dado, g.id_escola, g.ideb, {features},
       p.ideb_previsto AS ideb_previsto_bigquery
FROM `{projeto}.{dataset}.vw_gold_ideb_unificada` AS g
LEFT JOIN `{projeto}.{dataset}.predicoes_ideb_stream` AS p
  ON g.origem_dado = 'streaming' AND p.id_escola = g.id_escola AND p.ano = g.ano
WHERE g.id_escola = @id_escola AND (@ano IS NULL OR g.ano = @ano)
ORDER BY g.ano, g.origem_dado
LIMIT 20
"""

_cliente = None


def buscar_escola(projeto: str, dataset: str, id_escola: str, ano: Optional[int]) -> list[dict]:
    global _cliente
    from google.cloud import bigquery

    if _cliente is None:
        _cliente = bigquery.Client(project=projeto)
    sql = CONSULTA.format(projeto=projeto, dataset=dataset, features=", ".join(f"g.{c}" for c in NOMES))
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id_escola", "STRING", id_escola),
        bigquery.ScalarQueryParameter("ano", "INT64", ano),
    ])
    return [dict(r.items()) for r in _cliente.query(sql, job_config=cfg).result()]
