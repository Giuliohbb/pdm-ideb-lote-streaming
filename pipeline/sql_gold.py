"""Geração do SQL (BigQuery) das tabelas de streaming, das views de consolidação e do MERGE da gold.

Todo o SQL é gerado a partir de esquema.py para que as listas longas de colunas (87 na gold)
nunca sejam digitadas à mão. O mesmo código é usado:
  * por gerar_sql.py, que grava os arquivos da pasta sql/ para execução e inspeção;
  * por construir_gold.py, que roda o MERGE em micro-lotes;
  * pelos testes locais, que traduzem as views para DuckDB e conferem o gabarito.

`ref(nome)` devolve o nome qualificado da tabela, por exemplo `projeto.dataset.tabela`.
"""
from __future__ import annotations

from typing import Callable

import esquema as E

Ref = Callable[[str], str]


def ref_bigquery(projeto: str, dataset: str) -> Ref:
    return lambda nome: f"`{projeto}.{dataset}.{nome}`"


def _tipo_bq(t: str) -> str:
    return t


def _colunas_ddl(colunas) -> str:
    return ",\n  ".join(f"{c} {_tipo_bq(t)}" for c, t in colunas)


# ---------------------------------------------------------------------------
# DDL das tabelas de streaming
# ---------------------------------------------------------------------------
def ddl_tabelas(ref: Ref) -> list[str]:
    silver_cols = {
        "ideb": E.SILVER_IDEB, "censo_escola": E.SILVER_CENSO,
        "indicadores": E.SILVER_INDICADORES, "inse": E.SILVER_INSE, "docente": E.SILVER_DOCENTE,
    }
    descricao = {
        "ideb": "Silver de streaming do IDEB (append-only). Mesmas colunas da silver_ideb_escola + metadados.",
        "censo_escola": "Silver de streaming do Censo Escola (append-only). Mesmas 64 colunas da silver_censo_escola + metadados.",
        "indicadores": "Silver de streaming dos Indicadores (append-only). Mesmas colunas da silver_indicadores_escola + metadados.",
        "inse": "Silver de streaming do INSE (append-only). Inclui quantidade_alunos_inse, usada na deduplicacao.",
        "docente": "Silver de streaming dos docentes no grao docente x turma (append-only); a agregacao fica na view.",
    }
    out = [f"""CREATE TABLE IF NOT EXISTS {ref(E.TABELA_BRONZE_STREAM)} (
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  data STRING,
  attributes STRING
)
PARTITION BY DATE(publish_time)
OPTIONS (description = 'Bronze de streaming: mensagens cruas do Pub/Sub gravadas pela assinatura BigQuery (sem codigo).');"""]
    for tab in E.TABELAS:
        cols = silver_cols[tab] + E.METADADOS_SILVER
        out.append(f"""CREATE TABLE IF NOT EXISTS {ref(E.TABELA_SILVER_STREAM[tab])} (
  {_colunas_ddl(cols)}
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = '{descricao[tab]}');""")
    out.append(f"""CREATE TABLE IF NOT EXISTS {ref(E.TABELA_AUDITORIA)} (
  message_id STRING,
  publish_time TIMESTAMP,
  processado_em TIMESTAMP,
  tabela STRING,
  ref STRING,
  id_escola STRING,
  status STRING,
  motivo STRING,
  regras_aplicadas ARRAY<STRING>,
  payload STRING
)
PARTITION BY DATE(processado_em)
CLUSTER BY tabela, status
OPTIONS (description = 'Uma linha por mensagem processada: destino (aceito/descartado/quarentena), regras aplicadas e, na quarentena, o payload cru.');""")
    out.append(f"""CREATE TABLE IF NOT EXISTS {ref(E.TABELA_GOLD_STREAM)} (
  {_colunas_ddl(E.GOLD)},
  origem STRING,
  primeira_carga_em TIMESTAMP,
  atualizado_em TIMESTAMP,
  versao INT64,
  _hash INT64
)
PARTITION BY DATE(primeira_carga_em)
CLUSTER BY sigla_uf, id_escola
OPTIONS (description = 'Gold de streaming: mesmas 87 colunas da gold_ideb_features, consolidada por MERGE em micro-lotes.');""")
    out.append(f"""CREATE TABLE IF NOT EXISTS {ref(E.TABELA_PREDICOES)} (
  id_escola STRING,
  ano INT64,
  versao_gold INT64,
  ideb_real FLOAT64,
  ideb_previsto FLOAT64,
  erro_absoluto FLOAT64,
  modelo STRING,
  previsto_em TIMESTAMP
)
PARTITION BY DATE(previsto_em)
OPTIONS (description = 'Predicoes do modelo em producao para cada versao da gold de streaming.');""")
    return out


# ---------------------------------------------------------------------------
# Views de consolidação (deduplicação e agregação: as regras que precisam de estado)
# ---------------------------------------------------------------------------
def _lista(cols, alias: str | None = None) -> str:
    pref = f"{alias}." if alias else ""
    return ",\n  ".join(f"{pref}{c}" for c in cols)


def views(ref: Ref) -> dict[str, str]:
    """Devolve {nome_da_view: SELECT}. As views replicam a deduplicação da fase 1."""
    v = {}
    v["vw_silver_stream_ideb_escola"] = f"""SELECT id_escola, ano, ideb
FROM {ref(E.TABELA_SILVER_STREAM['ideb'])}
WHERE TRUE  -- o BigQuery exige WHERE, GROUP BY ou HAVING junto do QUALIFY
-- IDB06: duplicata conflitante -> fica o maior IDEB (mesma regra da fase 1)
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY ideb DESC, _publish_time DESC) = 1"""

    v["vw_silver_stream_censo_escola"] = f"""SELECT
  {_lista([c for c, _ in E.SILVER_CENSO])}
FROM {ref(E.TABELA_SILVER_STREAM['censo_escola'])}
WHERE TRUE
-- G01: reenvio -> fica a versao mais recente
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY _publish_time DESC, _ingestao_ts DESC) = 1"""

    v["vw_silver_stream_indicadores_escola"] = f"""SELECT
  {_lista([c for c, _ in E.SILVER_INDICADORES])}
FROM {ref(E.TABELA_SILVER_STREAM['indicadores'])}
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano ORDER BY _publish_time DESC, _ingestao_ts DESC) = 1"""

    v["vw_silver_stream_inse_escola"] = f"""SELECT id_escola, ano, inse
FROM {ref(E.TABELA_SILVER_STREAM['inse'])}
WHERE TRUE
-- INS03: mais de um INSE para a escola -> fica o calculado com mais alunos (regra da fase 1)
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano
                           ORDER BY quantidade_alunos_inse DESC NULLS LAST, _publish_time DESC) = 1"""

    agregados = ",\n  ".join(f"{expr} AS {col}" for col, _, expr in E.DOCENTE_AGREGADO)
    v["vw_silver_stream_censo_docente_agregado"] = f"""WITH vinculos AS (
  SELECT *
  FROM {ref(E.TABELA_SILVER_STREAM['docente'])}
  WHERE TRUE
  -- DOC03: o mesmo vinculo docente x turma reenviado conta uma vez so
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola, ano, id_docente, id_turma
                             ORDER BY _publish_time DESC, _ingestao_ts DESC) = 1
)
SELECT
  id_escola,
  ano,
  {agregados}
FROM vinculos
GROUP BY id_escola, ano"""

    gold_cols = ",\n  ".join(f"{c}" for c in E.GOLD_COLUNAS)
    v["vw_gold_ideb_unificada"] = f"""SELECT
  'lote' AS origem_dado,
  CAST(NULL AS TIMESTAMP) AS carregado_em,
  {gold_cols}
FROM {ref(E.TABELA_GOLD_LOTE)}
UNION ALL
SELECT
  'streaming' AS origem_dado,
  primeira_carga_em AS carregado_em,
  {gold_cols}
FROM {ref(E.TABELA_GOLD_STREAM)}"""

    v["vw_bronze_stream_eventos"] = f"""SELECT
  publish_time,
  message_id,
  JSON_VALUE(attributes, '$.tabela') AS tabela,
  JSON_VALUE(attributes, '$.ref') AS ref,
  JSON_VALUE(attributes, '$.escola_seq') AS escola_seq,
  data
FROM {ref(E.TABELA_BRONZE_STREAM)}"""
    return v


def ddl_views(ref: Ref) -> list[str]:
    return [f"CREATE OR REPLACE VIEW {ref(nome)} AS\n{sql};" for nome, sql in views(ref).items()]


# ---------------------------------------------------------------------------
# Consolidação da gold (micro-lote)
# ---------------------------------------------------------------------------
def _expr_gold(col: str) -> str:
    """De qual view vem cada coluna da gold."""
    if col in ("id_escola", "ano", "ideb"):
        return f"i.{col}"
    if col in [s for _, s, _ in E.CENSO_CONTEXTO] or col in [s for _, s, _ in E.CENSO_ATRIBUTOS]:
        if col != "qtd_docente_anos_finais":
            return f"c.{col}"
    if col in E.INDICADORES_NUMERICOS or col == E.INDICADOR_ICG:
        return f"x.{col}"
    if col == "inse":
        return "n.inse"
    return f"d.{col}"


def gold_candidatos(ref: Ref, atraso_segundos: int | None) -> str:
    """Escolas completas (5 fontes) e estáveis (nada novo chegou nos últimos N segundos)."""
    selecao = ",\n  ".join(f"{_expr_gold(c)} AS {c}" for c in E.GOLD_COLUNAS)
    chegadas = "\n  UNION ALL\n  ".join(
        f"SELECT id_escola, ano, _ingestao_ts FROM {ref(E.TABELA_SILVER_STREAM[t])}" for t in E.TABELAS)
    filtro = "" if atraso_segundos is None else \
        f"\nWHERE u.ultima_chegada <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(atraso_segundos)} SECOND)"
    return f"""WITH ultima AS (
  SELECT id_escola, ano, MAX(_ingestao_ts) AS ultima_chegada
  FROM (
  {chegadas}
  )
  GROUP BY id_escola, ano
)
SELECT
  {selecao}
FROM {ref('vw_silver_stream_ideb_escola')} AS i
JOIN {ref('vw_silver_stream_censo_escola')} AS c ON c.id_escola = i.id_escola AND c.ano = i.ano
JOIN {ref('vw_silver_stream_indicadores_escola')} AS x ON x.id_escola = i.id_escola AND x.ano = i.ano
JOIN {ref('vw_silver_stream_inse_escola')} AS n ON n.id_escola = i.id_escola AND n.ano = i.ano
JOIN {ref('vw_silver_stream_censo_docente_agregado')} AS d ON d.id_escola = i.id_escola AND d.ano = i.ano
JOIN ultima AS u ON u.id_escola = i.id_escola AND u.ano = i.ano{filtro}"""


def merge_gold(ref: Ref, atraso_segundos: int = 20) -> str:
    cols = E.GOLD_COLUNAS
    hash_expr = "FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(\n      " + ",\n      ".join(cols) + ")))"
    sets = ",\n    ".join(f"{c} = S.{c}" for c in cols if c not in ("id_escola", "ano"))
    ins_cols = ", ".join(cols)
    ins_vals = ", ".join(f"S.{c}" for c in cols)
    return f"""MERGE {ref(E.TABELA_GOLD_STREAM)} AS T
USING (
  SELECT *, {hash_expr} AS _hash
  FROM (
{_indentar(gold_candidatos(ref, atraso_segundos), 4)}
  )
) AS S
ON T.id_escola = S.id_escola AND T.ano = S.ano
WHEN MATCHED AND T._hash != S._hash THEN UPDATE SET
    {sets},
    _hash = S._hash,
    atualizado_em = CURRENT_TIMESTAMP(),
    versao = T.versao + 1
WHEN NOT MATCHED THEN INSERT ({ins_cols}, origem, primeira_carga_em, atualizado_em, versao, _hash)
VALUES ({ins_vals}, 'streaming', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 1, S._hash)"""


def merge_predicoes(ref: Ref, modelo: str) -> str:
    """Prediz com o modelo em produção só as versões da gold ainda não preditas."""
    return f"""MERGE {ref(E.TABELA_PREDICOES)} AS T
USING (
  SELECT id_escola, ano, versao AS versao_gold, ideb AS ideb_real, predicted_ideb AS ideb_previsto
  FROM ML.PREDICT(MODEL {ref(modelo)}, (
    SELECT g.*
    FROM {ref(E.TABELA_GOLD_STREAM)} AS g
    LEFT JOIN {ref(E.TABELA_PREDICOES)} AS p ON p.id_escola = g.id_escola AND p.ano = g.ano
    WHERE p.id_escola IS NULL OR p.versao_gold < g.versao
  ))
) AS S
ON T.id_escola = S.id_escola AND T.ano = S.ano
WHEN MATCHED THEN UPDATE SET
    versao_gold = S.versao_gold, ideb_real = S.ideb_real, ideb_previsto = S.ideb_previsto,
    erro_absoluto = ABS(S.ideb_real - S.ideb_previsto), modelo = '{modelo}', previsto_em = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (id_escola, ano, versao_gold, ideb_real, ideb_previsto, erro_absoluto, modelo, previsto_em)
VALUES (S.id_escola, S.ano, S.versao_gold, S.ideb_real, S.ideb_previsto, ABS(S.ideb_real - S.ideb_previsto),
        '{modelo}', CURRENT_TIMESTAMP())"""


def _indentar(txt: str, n: int) -> str:
    return "\n".join(" " * n + linha if linha else linha for linha in txt.splitlines())
