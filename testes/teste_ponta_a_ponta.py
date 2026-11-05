"""Teste ponta a ponta local, sem GCP.

Simula o caminho completo com os mesmos códigos que vão para a nuvem:
  produtor (ordem_eventos.csv)  ->  envelope push do Pub/Sub  ->  servico_limpeza (Flask)
  ->  silvers em memória  ->  views de consolidação + seleção da gold (SQL BigQuery
  traduzido para DuckDB com sqlglot)  ->  comparação com o gabarito do gerador.

Verificações:
  1. o destino de cada mensagem (aceito / descartado / quarentena) bate com o esperado;
  2. cada defeito injetado aparece nas regras aplicadas daquela mensagem;
  3. a gold reconstruída é idêntica à gold esperada (linhas, colunas e valores);
  4. todo o SQL BigQuery gerado (DDL, views, MERGE) é sintaticamente válido;
  5. a lista de colunas da gold é exatamente a do DDL de produção.

Uso: python testes/teste_ponta_a_ponta.py [--dados dados] [--limite-escolas N]
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import sqlglot

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "pipeline"))
import esquema as E  # noqa: E402
import servico_limpeza as SL  # noqa: E402
import sql_gold as SG  # noqa: E402


class GravadorMemoria:
    def __init__(self):
        self.tabelas = defaultdict(list)
        self.ids = defaultdict(set)

    def inserir(self, tabela, linhas, ids):
        for linha, i in zip(linhas, ids):
            if i in self.ids[tabela]:      # imita a deduplicação por insertId do BigQuery
                continue
            self.ids[tabela].add(i)
            self.tabelas[tabela].append(linha)


def ok(cond, msg):
    print(("  [OK]   " if cond else "  [FALHA] ") + msg)
    return bool(cond)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dados", default=str(RAIZ / "dados"))
    ap.add_argument("--limite-escolas", type=int, default=0)
    args = ap.parse_args()
    dados = Path(args.dados)
    tudo_ok = True

    # ------------------------------------------------------------------ 1. fluxo
    print("1) Enviando as mensagens ao serviço de limpeza (Flask test client)...")
    linhas = {t: (dados / "bronze" / f"{E.ARQUIVO_BRONZE[t]}.jsonl").read_text(encoding="utf-8").splitlines()
              for t in E.TABELAS}
    malformadas = [json.loads(x) for x in (dados / "bronze" / "mensagens_malformadas.jsonl").read_text().splitlines()]
    ordem = list(csv.DictReader(open(dados / "fluxo" / "ordem_eventos.csv", encoding="utf-8")))
    if args.limite_escolas:
        ordem = [o for o in ordem if int(o["escola_seq"]) < args.limite_escolas]

    grav = GravadorMemoria()
    SL._log = lambda *a, **k: None      # silencia os logs estruturados durante o teste
    app = SL.criar_app(grav)
    cli = app.test_client()
    base = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)
    obtido = {}
    for k, o in enumerate(ordem):
        tab, i = o["tabela"], int(o["linha"])
        if tab == "malformada":
            m = malformadas[i]
            dado, tabela_attr, ref = m["payload_bruto"], m["tabela"], f"malformada:{i}"
        else:
            dado, tabela_attr, ref = linhas[tab][i], tab, f"{tab}:{i}"
        env = {"message": {"data": base64.b64encode(dado.encode()).decode(), "messageId": str(k),
                           "publishTime": (base + timedelta(milliseconds=10 * k)).isoformat(),
                           "attributes": {"tabela": tabela_attr, "ref": ref, "escola_seq": o["escola_seq"]}},
               "subscription": "projects/x/subscriptions/teste"}
        r = cli.post("/pubsub", json=env)
        assert r.status_code == 204, (r.status_code, r.data)
    for a in grav.tabelas[E.TABELA_AUDITORIA]:
        obtido[a["ref"]] = a
    print(f"   {len(ordem)} mensagens processadas")

    # ------------------------------------------------------------------ 2. destinos
    print("2) Destino de cada mensagem x esperado")
    esperado = {}
    for r in csv.DictReader(open(dados / "gabarito" / "status_esperado.csv", encoding="utf-8")):
        esperado[f"{r['tabela']}:{r['linha']}"] = r
    refs = set(obtido)
    divergentes = [(ref, esperado[ref]["status"], obtido[ref]["status"], obtido[ref]["motivo"])
                   for ref in refs if esperado[ref]["status"] != obtido[ref]["status"]]
    tudo_ok &= ok(not divergentes, f"{len(refs)} mensagens, {len(divergentes)} com destino divergente")
    for d in divergentes[:10]:
        print("     ", d)
    cont = Counter((obtido[r]["tabela"], obtido[r]["status"]) for r in refs)
    for (t, s), n in sorted(cont.items()):
        print(f"     {t:<13} {s:<11} {n}")

    # ------------------------------------------------------------------ 3. regras
    print("3) Cada defeito injetado foi detectado pela regra correspondente")
    man = pd.read_csv(dados / "gabarito" / "manifesto_defeitos.csv", dtype=str)
    falhas_regra = []
    for _, m in man.iterrows():
        ref = f"{m['tabela']}:{m['linha']}"
        if ref not in obtido or m["codigo"] in ("G01", "IDB06", "INS03"):   # duplicatas: resolvidas nas views
            continue
        regras = [x.split("x")[0] for x in obtido[ref]["regras_aplicadas"]]
        if m["codigo"] not in regras:
            falhas_regra.append((ref, m["codigo"], obtido[ref]["regras_aplicadas"], m["valor_sujo"]))
    tudo_ok &= ok(not falhas_regra, f"{len(man)} defeitos no manifesto, {len(falhas_regra)} sem a regra esperada")
    for f in falhas_regra[:10]:
        print("     ", f)
    uso = Counter(x.split("x")[0] for a in grav.tabelas[E.TABELA_AUDITORIA] for x in a["regras_aplicadas"])
    print("     regras mais aplicadas:", dict(uso.most_common(12)))

    # ------------------------------------------------------------------ 4. gold
    print("4) Gold reconstruída (views + seleção da gold em DuckDB) x gabarito")
    con = duckdb.connect()
    tipos_duck = {"STRING": "VARCHAR", "INT64": "BIGINT", "FLOAT64": "DOUBLE", "TIMESTAMP": "TIMESTAMPTZ",
                  "ARRAY<STRING>": "VARCHAR[]"}
    silver_cols = {"ideb": E.SILVER_IDEB, "censo_escola": E.SILVER_CENSO, "indicadores": E.SILVER_INDICADORES,
                   "inse": E.SILVER_INSE, "docente": E.SILVER_DOCENTE}
    for t in E.TABELAS:
        nome = E.TABELA_SILVER_STREAM[t]
        cols = silver_cols[t] + E.METADADOS_SILVER
        con.execute(f"CREATE TABLE {nome} ({', '.join(f'{c} {tipos_duck[ty]}' for c, ty in cols)})")
        df = pd.DataFrame(grav.tabelas[nome], columns=[c for c, _ in cols])
        if len(df):
            con.register("tmp_df", df)
            con.execute(f"INSERT INTO {nome} SELECT * FROM tmp_df")
            con.unregister("tmp_df")
    ref_local = lambda n: n  # noqa: E731
    for nome, sql in SG.views(ref_local).items():
        if nome in ("vw_gold_ideb_unificada", "vw_bronze_stream_eventos"):
            continue
        duck = sqlglot.transpile(sql, read="bigquery", write="duckdb")[0]
        con.execute(f"CREATE VIEW {nome} AS {duck}")
    sel = sqlglot.transpile(SG.gold_candidatos(ref_local, None), read="bigquery", write="duckdb")[0]
    gold = con.execute(sel).df().sort_values("id_escola").reset_index(drop=True)
    gab = pd.read_csv(dados / "gabarito" / "gold_esperada.csv",
                      dtype={"id_escola": str, "id_municipio": str, "rede": str, "tipo_localizacao": str,
                             "tipo_localizacao_diferenciada": str})
    if args.limite_escolas:
        mapa = pd.read_csv(dados / "gabarito" / "mapa_semente.csv", dtype=str)
        gab = gab[gab["id_escola"].isin(mapa["id_escola_fluxo"].iloc[:args.limite_escolas])]
    gab = gab.sort_values("id_escola").reset_index(drop=True)
    tudo_ok &= ok(list(gold.columns) == E.GOLD_COLUNAS, "colunas da gold na ordem do DDL de produção")
    tudo_ok &= ok(len(gold) == len(gab), f"linhas: obtidas {len(gold)} x esperadas {len(gab)}")
    if len(gold) == len(gab):
        tudo_ok &= ok((gold["id_escola"].values == gab["id_escola"].values).all(), "mesmas escolas")
        difs = []
        for c in E.GOLD_COLUNAS:
            a, b = gold[c], gab[c]
            if pd.api.types.is_numeric_dtype(b):
                a, b = pd.to_numeric(a), pd.to_numeric(b)
                dif = ~(((a - b).abs() <= 1e-6) | (a.isna() & b.isna()))
            else:
                dif = ~((a.astype(str) == b.astype(str)) | (a.isna() & b.isna()))
            if dif.any():
                difs.append((c, int(dif.sum()), list(zip(a[dif][:3], b[dif][:3]))))
        tudo_ok &= ok(not difs, f"valores idênticos nas {len(E.GOLD_COLUNAS)} colunas ({len(difs)} colunas divergentes)")
        for d in difs[:10]:
            print("     ", d)

    # ------------------------------------------------------------------ 5. SQL BigQuery
    print("5) SQL BigQuery gerado é sintaticamente válido (sqlglot)")
    ref_bq = SG.ref_bigquery("platinum-avenue-505512-t5", "trabalho_pdm")
    stmts = SG.ddl_tabelas(ref_bq) + SG.ddl_views(ref_bq) + [SG.merge_gold(ref_bq, 20),
                                                              SG.merge_predicoes(ref_bq, "modelo_ideb")]
    erros = []
    for s in stmts:
        try:
            sqlglot.parse_one(s, read="bigquery")
        except Exception as exc:  # noqa: BLE001
            erros.append((s[:80], str(exc)[:200]))
    tudo_ok &= ok(not erros, f"{len(stmts)} comandos analisados, {len(erros)} com erro")
    for e in erros:
        print("     ", e)

    # ------------------------------------------------------------------ 6. DDL de produção
    print("6) Colunas da gold = DDL real de produção (semente/ddl_tabelas.csv)")
    import re
    ddl = {r["table_name"]: r["ddl"] for r in csv.DictReader(open(RAIZ / "semente" / "ddl_tabelas.csv"))}
    cols_prod = re.findall(r"^\s+([a-z_0-9]+) ([A-Z0-9]+),?$", ddl["gold_ideb_features"], re.M)
    tudo_ok &= ok(cols_prod == [(c, t) for c, t in E.GOLD], f"{len(cols_prod)} colunas, nomes, tipos e ordem idênticos")
    doc_prod = re.findall(r"^\s+([a-z_0-9]+) ([A-Z0-9]+),?$", ddl["silver_censo_docente_agregado"], re.M)[2:]
    tudo_ok &= ok(doc_prod == [(c, t) for c, t, _ in E.DOCENTE_AGREGADO], "agregado de docentes = silver de produção")

    # ------------------------------------------------------------------ 7. MERGE incremental
    print("7) MERGE incremental da gold (micro-lotes ao longo do fluxo, com dados tardios)")
    con2 = duckdb.connect()
    for t in E.TABELAS:
        nome = E.TABELA_SILVER_STREAM[t]
        cols = silver_cols[t] + E.METADADOS_SILVER
        con2.execute(f"CREATE TABLE {nome} ({', '.join(f'{c} {tipos_duck[ty]}' for c, ty in cols)})")
    for nome, sql in SG.views(ref_local).items():
        if nome in ("vw_gold_ideb_unificada", "vw_bronze_stream_eventos"):
            continue
        con2.execute(f"CREATE VIEW {nome} AS {sqlglot.transpile(sql, read='bigquery', write='duckdb')[0]}")
    gcols = ", ".join(f"{c} {tipos_duck[t]}" for c, t in E.GOLD)
    con2.execute(f"CREATE TABLE {E.TABELA_GOLD_STREAM} ({gcols}, origem VARCHAR, primeira_carga_em TIMESTAMPTZ, "
                 f"atualizado_em TIMESTAMPTZ, versao BIGINT, _hash UBIGINT)")
    merge = sqlglot.transpile(SG.merge_gold(ref_local, None), read="bigquery", write="duckdb")[0]
    merge = merge.replace("FARM_FINGERPRINT(", "hash(")
    passo = max(1000, len(ordem) // 15)
    carregado = {t: 0 for t in E.TABELAS}
    n_merges = 0
    for corte in list(range(passo, len(ordem), passo)) + [len(ordem)]:
        for t in E.TABELAS:
            nome = E.TABELA_SILVER_STREAM[t]
            cols = [c for c, _ in silver_cols[t] + E.METADADOS_SILVER]
            novas = [r for r in grav.tabelas[nome][carregado[t]:] if int(r["_message_id"]) < corte]
            if novas:
                con2.register("tmp_df", pd.DataFrame(novas, columns=cols))
                con2.execute(f"INSERT INTO {nome} SELECT * FROM tmp_df")
                con2.unregister("tmp_df")
                carregado[t] += len(novas)
        con2.execute(merge)
        n_merges += 1
    final = con2.execute(f"SELECT * FROM {E.TABELA_GOLD_STREAM} ORDER BY id_escola").df()
    versoes = final["versao"].value_counts().sort_index().to_dict()
    iguais = len(final) == len(gab) and all(
        ((pd.to_numeric(final[c], errors="coerce") - pd.to_numeric(gab[c], errors="coerce")).abs().fillna(0) <= 1e-6).all()
        if pd.api.types.is_numeric_dtype(gab[c]) else (final[c].astype(str).values == gab[c].astype(str).values).all()
        for c in E.GOLD_COLUNAS)
    tudo_ok &= ok(iguais, f"{n_merges} micro-lotes; gold final igual ao gabarito; linhas por versão: {versoes}")

    print("\nRESULTADO:", "TUDO CERTO" if tudo_ok else "HÁ FALHAS")
    sys.exit(0 if tudo_ok else 1)


if __name__ == "__main__":
    main()
