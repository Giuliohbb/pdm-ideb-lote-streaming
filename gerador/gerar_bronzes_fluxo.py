"""Gerador das 5 bronzes do fluxo de streaming do Trabalho 2 (IDEB, Censo Escola, Indicadores, INSE, Docente).

O que são estes dados, com todas as letras: registros reais de 2019 do INEP (via Base dos Dados),
com ruído e defeitos controlados para exercitar a limpeza. Cada escola do fluxo deriva de uma escola
real de 2019 e é rotulada com o ano informado em --ano (2023 por padrão, para simular um ano novo).
O corpo docente é reconstruído linha a linha a partir das proporções reais da escola e da amostra
real de docentes de 2019.

Como funciona
-------------
1. Parte da SEMENTE REAL: ~3.100 escolas de 2019 extraídas da gold da fase 1, com as linhas
   brutas das cinco bronzes dessas mesmas escolas (consultas do arquivo extracao_semente_real.sql).
2. Sorteia N escolas preservando a distribuição por região x quintil de INSE x rede x localização.
3. Para cada escola sorteada cria uma escola do fluxo: código próprio, ruído moderado nas quantidades,
   nos indicadores, no INSE e no IDEB, e algumas flags de infraestrutura invertidas. UF, município,
   rede e localização são mantidos (preservam a distribuição regional e socioeconômica).
4. Gera o corpo docente linha a linha (grão docente x turma), reproduzindo as proporções reais
   da escola (superior, licenciatura, pós, efetivos, matemática, português).
5. Injeta os DEFEITOS do catálogo (docs/CATALOGO_DEFEITOS.md) sobre uma cópia suja e registra cada
   um no manifesto. Os defeitos são a sujeira que o pipeline de limpeza precisa resolver ao vivo.
6. Calcula o GABARITO: a gold que um pipeline correto deve produzir a partir dos dados sujos,
   usando uma implementação de referência das regras da fase 1 escrita aqui, em pandas,
   independente do código do serviço de limpeza. Os testes comparam uma coisa com a outra.

Saídas (pasta --saida)
----------------------
  bronze/bronze_*.jsonl               as 5 bronzes sujas (mesmos nomes da fase 1), uma linha JSON por registro
  bronze/mensagens_malformadas.jsonl  mensagens com JSON corrompido (testam a quarentena)
  fluxo/ordem_eventos.csv             ordem de envio usada pelo produtor (escola a escola)
  gabarito/gold_esperada.csv          a gold que o pipeline deve produzir
  gabarito/status_esperado.csv        destino esperado de cada mensagem (aceito/descartado/quarentena)
  gabarito/manifesto_defeitos.csv     cada defeito injetado: linha, coluna, valor limpo e valor sujo
  gabarito/mapa_semente.csv           escola do fluxo -> escola real de 2019 que lhe deu origem
  gabarito/resumo_esperado.json       contagens esperadas por tabela, destino e código de regra

Uso
---
  python gerador/gerar_bronzes_fluxo.py --semente semente --saida dados --n-escolas 2000 --ano 2023
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "pipeline"))
import esquema as E  # noqa: E402  (apenas listas de colunas e domínios; nenhuma regra de limpeza)

ESQUEMAS = json.loads((Path(__file__).with_name("esquemas_bronze.json")).read_text(encoding="utf-8"))
ARQ_SEMENTE = {
    "gold": "semente_gold_2019.csv",
    "ideb": "bronze_ideb_2019.csv",
    "censo_escola": "bronze_censo_escola_2019.csv",
    "indicadores": "bronze_indicadores_2019.csv",
    "inse": "bronze_inse_2019.csv",
    "docente": "bronze_docente_amostra.csv",
}
REDE_PALAVRA = {"1": "federal", "2": "estadual", "3": "municipal", "4": "privada"}
CODIGO_DA_UF = {v: k for k, v in E.CODIGO_UF.items()}
ETAPAS_OUTRAS = ["14", "15", "16", "17", "18", "25", "26", "27", "70", "71"]


# =============================================================================
# Utilidades
# =============================================================================
def tipar(valor: str, tipo: str):
    """Converte o texto exportado do BigQuery para o tipo da coluna ('' vira None)."""
    if valor is None or valor == "":
        return None
    if tipo == "int64":
        return int(float(valor))
    if tipo == "float64":
        return float(valor)
    return valor


def carregar(dir_semente: Path) -> dict[str, list[dict]]:
    dados = {}
    for chave, arq in ARQ_SEMENTE.items():
        df = pd.read_csv(dir_semente / arq, dtype=str, keep_default_na=False)
        if chave == "gold":
            dados[chave] = df
            continue
        tipos = dict(ESQUEMAS[chave])
        dados[chave] = [{c: tipar(v, tipos[c]) for c, v in r.items() if c in tipos}
                        for r in df.to_dict("records")]
    return dados


def arred(x, casas):
    """Arredondamento 'meio para longe do zero', igual ao ROUND do BigQuery."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    f = 10 ** casas
    return math.copysign(math.floor(abs(x) * f + 0.5), x) / f


def ajustar_soma(valores: list[float | None], alvo: float, casas: int = 1) -> list[float | None]:
    """Arredonda mantendo a soma (percentuais por grupo/nível devem somar 100)."""
    idx = [i for i, v in enumerate(valores) if v is not None]
    if not idx:
        return valores
    tot = sum(valores[i] for i in idx)
    if tot <= 0:
        return valores
    novos = [None if v is None else max(0.0, v) * alvo / tot for v in valores]
    novos = [None if v is None else arred(v, casas) for v in novos]
    dif = arred(alvo - sum(novos[i] for i in idx), casas)
    imax = max(idx, key=lambda i: novos[i])
    novos[imax] = arred(novos[imax] + dif, casas)
    return novos


# =============================================================================
# 1. Amostragem estratificada da semente
# =============================================================================
def amostrar_escolas(gold: pd.DataFrame, n: int, rng: np.random.Generator) -> list[str]:
    estrato = gold["regiao"] + "|" + gold["quintil_inse"] + "|" + gold["rede"] + "|" + gold["tipo_localizacao"]
    ids = gold["id_escola"].to_numpy()
    if n >= len(gold):
        base = list(ids)
        extra = rng.choice(ids, size=n - len(gold), replace=True).tolist() if n > len(gold) else []
        escolhidos = base + extra
    else:
        escolhidos = []
        grupos = gold.groupby(estrato).indices
        cotas = {k: n * len(v) / len(gold) for k, v in grupos.items()}
        inteiros = {k: int(math.floor(c)) for k, c in cotas.items()}
        sobra = n - sum(inteiros.values())
        for k in sorted(cotas, key=lambda k: cotas[k] - inteiros[k], reverse=True)[:sobra]:
            inteiros[k] += 1
        for k, pos in grupos.items():
            q = min(inteiros[k], len(pos))
            if q:
                escolhidos += rng.choice(ids[pos], size=q, replace=False).tolist()
    rng.shuffle(escolhidos)
    return escolhidos


# =============================================================================
# 2. Escola do fluxo, ainda limpa (ruído controlado sobre a escola real de 2019)
# =============================================================================
class Fabrica:
    def __init__(self, sem: dict, ano: int, rng: np.random.Generator, vinc_p: float):
        self.rng, self.ano, self.vinc_p = rng, ano, vinc_p
        self.gold = sem["gold"].set_index("id_escola")
        self.ideb = defaultdict(list)
        for r in sem["ideb"]:
            self.ideb[r["id_escola"]].append(r)
        self.censo = {r["id_escola"]: r for r in sem["censo_escola"]}
        self.ind = {r["id_escola"]: r for r in sem["indicadores"]}
        self.inse = {r["id_escola"]: r for r in sem["inse"]}
        doc = [r for r in sem["docente"] if r.get("rede") != "privada"]   # só escolas públicas
        af = [r for r in doc if r.get("etapa_ensino") in E.ETAPAS_ANOS_FINAIS]
        self.tpl_sup = [r for r in af if r.get("escolaridade") == "4"]
        self.tpl_nsup = [r for r in af if r.get("escolaridade") != "4"] or self.tpl_sup
        self.tpl_outras = [r for r in doc if r.get("etapa_ensino") not in E.ETAPAS_ANOS_FINAIS
                           and r.get("etapa_ensino") is not None]
        self.tpl_sem_etapa = [r for r in doc if r.get("etapa_ensino") is None]
        self.seq_uf = Counter()
        self.seq_turma = 0

    # ---- identificadores --------------------------------------------------
    def novo_id(self, uf: str) -> str:
        self.seq_uf[uf] += 1
        return f"{CODIGO_DA_UF[uf]}99{self.seq_uf[uf]:04d}"

    def novo_docente(self) -> str:
        return uuid.UUID(bytes=self.rng.bytes(16)).hex.upper()

    def nova_turma(self) -> str:
        self.seq_turma += 1
        return f"9{self.seq_turma:07d}"

    # ---- escola -----------------------------------------------------------
    def escola(self, id_semente: str, seq: int) -> dict:
        g = self.gold.loc[id_semente]
        uf = g["sigla_uf"]
        novo = self.novo_id(uf)
        f_mat = float(math.exp(self.rng.normal(0, 0.08)))   # fator de tamanho (matrículas, turmas)
        f_doc = float(math.exp(self.rng.normal(0, 0.05)))
        comum = {"ano": self.ano, "id_escola": novo}
        linhas = {
            "ideb": self._ideb(id_semente, comum),
            "censo_escola": [self._censo(id_semente, comum, f_mat, f_doc)],
            "indicadores": [self._indicadores(id_semente, comum)],
            "inse": [self._inse(id_semente, comum, f_mat)],
        }
        n_turmas = linhas["censo_escola"][0].get("quantidade_turma_fundamental_anos_finais") or 1
        linhas["docente"] = self._docentes(g, comum, f_doc, max(1, n_turmas))
        for tab, lst in linhas.items():
            for r in lst:
                r["_escola_seq"] = seq
        return {"id_escola": novo, "id_semente": id_semente, "regiao": g["regiao"],
                "quintil_inse": g["quintil_inse"], "linhas": linhas}

    def _ideb(self, sid, comum):
        out = []
        for r in self.ideb[sid]:
            r = dict(r, **comum)
            if r.get("ideb") is not None:
                velho = r["ideb"]
                novo = arred(min(9.5, max(0.5, velho + self.rng.normal(0, 0.15))), 1)
                r["ideb"] = novo
                p = r.get("indicador_rendimento")
                n_velho = r.get("nota_saeb_media_padronizada")
                if p and n_velho is not None:
                    n_novo = min(10.0, novo / p)
                    delta = 30 * (n_novo - n_velho)
                    r["nota_saeb_media_padronizada"] = arred(n_novo, 7)
                    for c in ("nota_saeb_matematica", "nota_saeb_lingua_portuguesa"):
                        if r.get(c) is not None:
                            r[c] = arred(r[c] + delta, 2)
            out.append(r)
        return out

    def _censo(self, sid, comum, f_mat, f_doc):
        r = dict(self.censo[sid], **comum)
        for c in ("data_ano_letivo_inicio", "data_ano_letivo_termino"):
            if r.get(c):
                r[c] = f"{self.ano}{r[c][4:]}"
        for c, v in list(r.items()):
            if v is None or not isinstance(v, int) or isinstance(v, bool):
                continue
            if c.startswith(("quantidade_matricula", "quantidade_turma", "quantidade_sala_utilizada")):
                r[c] = max(1 if v >= 1 else 0, int(round(v * f_mat)))
            elif c.startswith("quantidade_docente"):
                r[c] = max(1 if v >= 1 else 0, int(round(v * f_doc)))
        estruturais = {"local_funcionamento_predio_escolar", "alimentacao", "diurno", "noturno"}
        for bronze, _, _ in E.CENSO_ATRIBUTOS:
            if (not E.censo_eh_quantidade(bronze) and bronze not in estruturais
                    and not bronze.startswith("etapa_") and r.get(bronze) in (0, 1)
                    and self.rng.random() < 0.02):
                r[bronze] = 1 - r[bronze]
        return r

    def _indicadores(self, sid, comum):
        r = dict(self.ind[sid], **comum)
        a = float(math.exp(self.rng.normal(0, 0.03)))
        h = float(self.rng.normal(0, 0.08))
        for c, v in list(r.items()):
            if v is None:
                continue
            if c.startswith("atu_"):
                r[c] = arred(v * a, 1)
            elif c.startswith("had_"):
                r[c] = arred(max(1.0, v + h), 1)
        grupos = [f"afd_ef_anos_finais_grupo_{k}" for k in range(1, 6)]
        vals = [r.get(c) for c in grupos]
        if vals[0] is not None:
            vals[0] = min(100.0, max(0.0, vals[0] + self.rng.normal(0, 2.5)))
            soma_outros = sum(v for v in vals[1:] if v is not None)
            resto = 100.0 - vals[0]
            if soma_outros > 0:
                vals = [vals[0]] + [None if v is None else v * resto / soma_outros for v in vals[1:]]
            vals = ajustar_soma(vals, 100.0)
            for c, v in zip(grupos, vals):
                r[c] = v
        niveis = [f"ied_ef_anos_finais_nivel_{k}" for k in range(1, 7)]
        vals = [r.get(c) for c in niveis]
        if any(v for v in vals if v is not None):
            vals = [None if v is None else (v + self.rng.normal(0, 1.5) if v > 0 else 0.0) for v in vals]
            vals = ajustar_soma(vals, 100.0)
            for c, v in zip(niveis, vals):
                r[c] = v
        if r.get("ird_media_regularidade_docente") is not None:
            v = r["ird_media_regularidade_docente"] + self.rng.normal(0, 0.06)
            r["ird_media_regularidade_docente"] = arred(min(5.0, max(1.0, v)), 6)
        return r

    def _inse(self, sid, comum, f_mat):
        r = dict(self.inse[sid], **comum)
        if r.get("inse") is not None:
            r["inse"] = arred(r["inse"] + self.rng.normal(0, 0.04), 2)
        if r.get("quantidade_alunos_inse"):
            r["quantidade_alunos_inse"] = max(1, int(round(r["quantidade_alunos_inse"] * f_mat)))
        return r

    # ---- docentes (grão docente x turma) ---------------------------------
    def _docentes(self, g, comum, f_doc, n_turmas):
        rng = self.rng
        n_sem = max(1, int(g["qtd_docente_anos_finais"]))
        n = max(1, int(round(n_sem * f_doc)))

        def prop(col):
            return min(1.0, max(0.0, int(g[col]) / n_sem))

        def sortear(qtd, universo):
            universo = list(universo)
            qtd = max(0, min(len(universo), qtd))
            return set(rng.choice(universo, size=qtd, replace=False).tolist()) if qtd else set()

        todos = range(n)
        sup = sortear(round(prop("qtd_docente_escolaridade_superior") * n), todos)
        lic = sortear(round(prop("qtd_docente_licenciatura") * n), sup)
        esp = sortear(round(prop("qtd_docente_especializacao") * n), sup)
        mes = sortear(round(prop("qtd_docente_mestrado") * n), sup)
        dou = sortear(round(prop("qtd_docente_doutorado") * n), sup)
        efe = sortear(round(prop("qtd_docente_efetivo") * n), todos)
        reg = sortear(max(1, round(prop("qtd_docente_tipo_docente_regular") * n)), todos)
        mat = sortear(round(prop("qtd_docente_disciplina_matematica") * n), reg)
        por = sortear(round(prop("qtd_docente_disciplina_lingua_portuguesa") * n), set(reg) - mat or reg)
        idade_alvo = float(g["idade_media_docente"]) + rng.normal(0, 0.4)

        turmas = {e: [self.nova_turma() for _ in range(max(1, n_turmas // 4 + (1 if i < n_turmas % 4 else 0)))]
                  for i, e in enumerate(sorted(E.ETAPAS_ANOS_FINAIS))}
        uf, mun = g["sigla_uf"], g["id_municipio"]
        rede = REDE_PALAVRA.get(g["rede"], "municipal")
        dif = g["tipo_localizacao_diferenciada"]
        dif_doc = str(int(float(dif))) if dif not in ("", None) else None
        base_escola = dict(comum, sigla_uf=uf, id_municipio=mun, rede=rede,
                           tipo_localizacao=g["tipo_localizacao"], tipo_localizacao_diferenciada=dif_doc)

        linhas = []
        for i in todos:
            superior = i in sup
            tpl = dict(self.tpl_sup[rng.integers(len(self.tpl_sup))] if superior
                       else self.tpl_nsup[rng.integers(len(self.tpl_nsup))])
            idade = int(min(68, max(22, round(rng.normal(idade_alvo, 7)))))
            d = dict(tpl, **base_escola)
            d.update({
                "id_docente": self.novo_docente(),
                "idade": idade, "idade_referencia": idade, "ano_nascimento_docente": self.ano - idade,
                "escolaridade": "4" if superior else (tpl.get("escolaridade") if tpl.get("escolaridade") != "4" else "3"),
                "licenciatura_1": 1 if i in lic else 0,
                "especializacao": (1 if i in esp else 0) if superior else None,
                "mestrado": (1 if i in mes else 0) if superior else None,
                "doutorado": (1 if i in dou else 0) if superior else None,
                "tipo_contratacao": "1" if i in efe else str(rng.choice(["2", "2", "2", "4"])),
                "tipo_docente": 1 if i in reg else int(rng.choice([2, 3])),
                "disciplina_matematica": 1 if i in mat else 0,
                "disciplina_lingua_portuguesa": 1 if i in por else 0,
                "regular": 1,
            })
            if superior:
                d["pos_nenhum"] = 0 if (i in esp or i in mes or i in dou) else 1
            else:
                d["pos_nenhum"] = None
            k = 1 + int(rng.binomial(2, self.vinc_p))
            etapas = rng.choice(sorted(E.ETAPAS_ANOS_FINAIS), size=k, replace=True)
            usadas = set()
            for e in etapas:
                t = turmas[e][int(rng.integers(len(turmas[e])))]
                if t in usadas:
                    continue
                usadas.add(t)
                linhas.append(dict(d, etapa_ensino=e, id_turma=t))
            if rng.random() < 0.15:  # o mesmo professor também leciona em outra etapa
                linhas.append(dict(d, etapa_ensino=str(rng.choice(ETAPAS_OUTRAS)), id_turma=self.nova_turma()))
        # docentes que só atuam em outras etapas e linhas sem etapa (AEE/atividade complementar)
        for _ in range(int(rng.binomial(n, 0.10))):
            tpl = dict(self.tpl_outras[rng.integers(len(self.tpl_outras))])
            idade = int(min(68, max(22, round(rng.normal(idade_alvo, 9)))))
            linhas.append(dict(tpl, **base_escola, id_docente=self.novo_docente(), idade=idade,
                               idade_referencia=idade, ano_nascimento_docente=self.ano - idade,
                               id_turma=self.nova_turma()))
        for _ in range(int(rng.binomial(n, 0.05))):
            tpl = dict(self.tpl_sem_etapa[rng.integers(len(self.tpl_sem_etapa))])
            idade = int(min(68, max(22, round(rng.normal(idade_alvo, 9)))))
            linhas.append(dict(tpl, **base_escola, id_docente=self.novo_docente(), idade=idade,
                               idade_referencia=idade, ano_nascimento_docente=self.ano - idade,
                               etapa_ensino=None, id_turma=self.nova_turma()))
        return linhas


# =============================================================================
# 3. Implementação de referência das regras da fase 1 (pandas) -> gabarito
# =============================================================================
def referencia_gold(esp: dict[str, list[dict]]) -> pd.DataFrame:
    """Aplica as regras da fase 1 às linhas esperadas (limpas) e devolve a gold esperada."""
    ide = pd.DataFrame(esp["ideb"])
    ide = ide[(ide["ensino"] == E.IDEB_ENSINO_ALVO) & (ide["anos_escolares"] == E.IDEB_ANOS_ESCOLARES_ALVO)
              & ide["ideb"].notna()]
    ide = ide.sort_values("ideb", ascending=False).drop_duplicates(["id_escola", "ano"])[["id_escola", "ano", "ideb"]]

    cen = pd.DataFrame(esp["censo_escola"]).drop_duplicates(["id_escola", "ano"])
    cen_out = cen[["id_escola", "ano"]].copy()
    for b, s, _ in E.CENSO_CONTEXTO:
        cen_out[s] = cen[b]
    for b, s, _ in E.CENSO_ATRIBUTOS:
        cen_out[s] = cen[b].fillna(0).astype(int) if b in cen else 0

    ind = pd.DataFrame(esp["indicadores"]).drop_duplicates(["id_escola", "ano"])
    ind = ind[["id_escola", "ano"] + E.INDICADORES_NUMERICOS + [E.INDICADOR_ICG]]

    ins = pd.DataFrame(esp["inse"])
    ins = ins[ins["inse"].notna()].sort_values("quantidade_alunos_inse", ascending=False)
    ins = ins.drop_duplicates(["id_escola", "ano"])[["id_escola", "ano", "inse"]]

    doc = pd.DataFrame(esp["docente"])
    doc = doc[doc["etapa_ensino"].isin(E.ETAPAS_ANOS_FINAIS)]
    doc = doc.drop_duplicates(["id_escola", "ano", "id_docente", "id_turma"])

    def agg(gp):
        def cnt(mask):
            return gp.loc[mask, "id_docente"].nunique()
        idade = gp["idade"].dropna()
        return pd.Series({
            "qtd_docente_anos_finais": gp["id_docente"].nunique(),
            "idade_media_docente": arred(float(idade.mean()), 1) if len(idade) else None,
            "qtd_docente_escolaridade_superior": cnt(gp["escolaridade"] == "4"),
            "qtd_docente_licenciatura": cnt(gp["licenciatura_1"] == 1),
            "qtd_docente_especializacao": cnt(gp["especializacao"] == 1),
            "qtd_docente_mestrado": cnt(gp["mestrado"] == 1),
            "qtd_docente_doutorado": cnt(gp["doutorado"] == 1),
            "qtd_docente_efetivo": cnt(gp["tipo_contratacao"] == "1"),
            "qtd_docente_tipo_docente_regular": cnt(gp["tipo_docente"] == 1),
            "qtd_docente_disciplina_matematica": cnt(gp["disciplina_matematica"] == 1),
            "qtd_docente_disciplina_lingua_portuguesa": cnt(gp["disciplina_lingua_portuguesa"] == 1),
        })
    dag = doc.groupby(["id_escola", "ano"]).apply(agg, include_groups=False).reset_index()

    gold = (ide.merge(cen_out.drop(columns=["qtd_docente_anos_finais"]), on=["id_escola", "ano"])
               .merge(ind, on=["id_escola", "ano"]).merge(ins, on=["id_escola", "ano"])
               .merge(dag, on=["id_escola", "ano"]))
    return gold[E.GOLD_COLUNAS].sort_values("id_escola").reset_index(drop=True)


# =============================================================================
# 4. Injeção de defeitos
# =============================================================================
class Injetor:
    """Cada método recebe a linha limpa e devolve a versão suja.

    Registra no manifesto o código, a coluna, o valor limpo e o valor sujo, e o efeito do
    defeito sobre o gabarito: 'nenhum' (a limpeza recupera o valor), 'remover' (a linha vai
    para descarte/quarentena) ou 'nulo:<coluna>' (a limpeza só consegue anular o campo).
    """

    def __init__(self, rng: np.random.Generator, escala: float):
        self.rng, self.escala = rng, escala

    def p(self, base: float) -> bool:
        return self.rng.random() < min(1.0, base * self.escala)

    def escolha(self, opcoes):
        return opcoes[int(self.rng.integers(len(opcoes)))]

    # ---- genéricos -------------------------------------------------------
    def id_sujo(self, r):
        v = r["id_escola"]
        sujo = self.escolha([int(v), f" {v} ", f"{v[:2]}.{v[2:5]}.{v[5:]}", f"{v}\t"])
        return dict(r, id_escola=sujo), ("G02", "id_escola", v, sujo, "aceito", "nenhum")

    def id_ausente(self, r):
        return dict(r, id_escola=self.escolha([None, "", "NA"])), ("G03", "id_escola", r["id_escola"], None, "quarentena", "remover")

    def ano_sujo(self, r):
        sujo = self.escolha([str(r["ano"]), float(r["ano"]), f" {r['ano']} "])
        return dict(r, ano=sujo), ("G04", "ano", r["ano"], sujo, "aceito", "nenhum")

    def campo_extra(self, r):
        nome = self.escolha(["campo_inesperado_v2", "observacao_operador", "versao_layout"])
        return dict(r, **{nome: self.escolha(["x", 1, "layout-2024"])}), ("G07", nome, None, "campo novo", "aceito", "nenhum")

    # ---- IDEB ------------------------------------------------------------
    def ideb_caixa(self, r):
        e = self.escolha(["Fundamental", " FUNDAMENTAL", "fundamental "])
        a = self.escolha(["Finais (6-9)", "FINAIS (6-9) ", "finais  (6-9)"])
        return dict(r, ensino=e, anos_escolares=a), ("IDB02", "ensino/anos_escolares", "fundamental/finais (6-9)", f"{e}/{a}", "aceito", "nenhum")

    def ideb_suprimido(self, r):
        sujo = self.escolha([None, "-", "*", ""])
        return dict(r, ideb=sujo), ("IDB03", "ideb", r["ideb"], sujo, "descartado", "remover")

    def ideb_virgula(self, r):
        sujo = f"{r['ideb']:.1f}".replace(".", ",")
        return dict(r, ideb=sujo), ("IDB04", "ideb", r["ideb"], sujo, "aceito", "nenhum")

    def ideb_fora(self, r):
        sujo = arred(r["ideb"] * 10, 1)
        return dict(r, ideb=sujo), ("IDB05", "ideb", r["ideb"], sujo, "quarentena", "remover")

    # ---- Censo -----------------------------------------------------------
    def _zeros(self, r):
        return [b for b, _, _ in E.CENSO_ATRIBUTOS if r.get(b) == 0]

    def censo_nulos(self, r):
        zeros = self._zeros(r)
        if not zeros:
            return None
        cols = self.rng.choice(zeros, size=min(len(zeros), 1 + int(self.rng.integers(4))), replace=False)
        sujo = dict(r)
        for c in cols:
            if self.rng.random() < 0.5:
                sujo.pop(c, None)   # chave ausente no JSON
            else:
                sujo[c] = ""        # texto vazio
        return sujo, ("CEN01", ",".join(cols), 0, "nulo/vazio", "aceito", "nenhum")

    def censo_flag_texto(self, r):
        flags = [b for b, _, _ in E.CENSO_ATRIBUTOS if not E.censo_eh_quantidade(b) and r.get(b) in (0, 1)]
        cols = self.rng.choice(flags, size=min(len(flags), 1 + int(self.rng.integers(3))), replace=False)
        sujo = dict(r)
        for c in cols:
            sujo[c] = self.escolha(["Sim", "S", "true", "1.0", True]) if r[c] == 1 else \
                self.escolha(["Não", "N", "false", "0.0", False])
        return sujo, ("CEN02", ",".join(cols), "0/1", "texto", "aceito", "nenhum")

    def censo_sentinela(self, r):
        zeros = self._zeros(r)
        if not zeros:
            return None
        c = self.escolha(zeros)
        sujo = self.escolha([-1, "-1"])
        return dict(r, **{c: sujo}), ("CEN03", c, 0, sujo, "aceito", "nenhum")

    def censo_categoria(self, r):
        alvo = self.escolha(["rede", "tipo_localizacao", "tipo_localizacao_diferenciada"])
        v = r.get(alvo)
        if v is None:
            return None
        if alvo == "rede":
            palavra = REDE_PALAVRA[v]
            sujo = self.escolha([palavra.capitalize(), palavra.upper(), f" {palavra}", f"{v}.0", int(v)])
        elif alvo == "tipo_localizacao":
            palavra = "urbana" if v == "1" else "rural"
            sujo = self.escolha([palavra.capitalize(), palavra.upper(), int(v)])
        else:
            sujo = self.escolha([str(int(float(v))), int(float(v))])
        return dict(r, **{alvo: sujo}), ("CEN04", alvo, v, sujo, "aceito", "nenhum")

    def censo_quantidade_texto(self, r):
        qs = [b for b, _, _ in E.CENSO_ATRIBUTOS if E.censo_eh_quantidade(b) and isinstance(r.get(b), int)]
        if not qs:
            return None
        c = self.escolha(qs)
        v = r[c]
        sujo = f"{v:,}".replace(",", ".") if v >= 1000 else self.escolha([f"{v}.0", str(v)])
        return dict(r, **{c: sujo}), ("CEN05", c, v, sujo, "aceito", "nenhum")

    def censo_municipio(self, r):
        v = r["id_municipio"]
        sujo = self.escolha([int(v), f"{v}.0", f" {v} "])
        return dict(r, id_municipio=sujo), ("CEN06", "id_municipio", v, sujo, "aceito", "nenhum")

    def censo_uf(self, r):
        v = r["sigla_uf"]
        sujo = self.escolha([v.lower(), f" {v} ", v.capitalize()])
        return dict(r, sigla_uf=sujo), ("G05", "sigla_uf", v, sujo, "aceito", "nenhum")

    # ---- Indicadores -----------------------------------------------------
    def ind_virgula(self, r):
        cols = [c for c in E.INDICADORES_NUMERICOS if r.get(c) is not None]
        if not cols:
            return None
        cols = self.rng.choice(cols, size=min(len(cols), 1 + int(self.rng.integers(3))), replace=False)
        sujo = dict(r)
        for c in cols:
            sujo[c] = repr(float(r[c])).replace(".", ",")
        return sujo, ("IND02", ",".join(cols), "decimal", "virgula", "aceito", "nenhum")

    def ind_percentual(self, r):
        cols = [c for c in E.INDICADORES_NUMERICOS if c.startswith(("afd", "ied")) and r.get(c) is not None]
        if not cols:
            return None
        c = self.escolha(cols)
        sujo = self.escolha([f"{r[c]}%", repr(float(r[c])).replace(".", ",") + "%"])
        return dict(r, **{c: sujo}), ("IND03", c, r[c], sujo, "aceito", "nenhum")

    def ind_icg(self, r):
        v = r.get(E.INDICADOR_ICG)
        if v is None:
            return None
        n = v.split()[-1]
        sujo = self.escolha([f"Nível {n}", f"NIVEL {n}", f"nivel_{n}", n, int(n)])
        return dict(r, **{E.INDICADOR_ICG: sujo}), ("IND04", E.INDICADOR_ICG, v, sujo, "aceito", "nenhum")

    def ind_fora(self, r):
        opcoes = [c for c in ("afd_ef_anos_finais_grupo_1", "atu_ef_anos_finais", "had_ef_anos_finais")
                  if r.get(c) is not None]
        if not opcoes:
            return None
        c = self.escolha(opcoes)
        sujo = {"afd_ef_anos_finais_grupo_1": arred(r[c] + 100.0, 1), "atu_ef_anos_finais": -r[c],
                "had_ef_anos_finais": 99.0}[c]
        return dict(r, **{c: sujo}), ("IND05", c, r[c], sujo, "aceito", f"nulo:{c}")

    def ind_marcador(self, r):
        nulos = [c for c in E.INDICADORES_NUMERICOS if r.get(c) is None]
        if not nulos:
            return None
        sujo = dict(r)
        for c in nulos:
            sujo[c] = self.escolha(["--", "", "-"])
        return sujo, ("IND01", ",".join(nulos), None, "marcador", "aceito", "nenhum")

    # ---- INSE ------------------------------------------------------------
    def inse_nulo(self, r):
        sujo = self.escolha([None, "", "-"])
        return dict(r, inse=sujo), ("INS01", "inse", r["inse"], sujo, "descartado", "remover")

    def inse_virgula(self, r):
        sujo = f"{r['inse']:.2f}".replace(".", ",")
        return dict(r, inse=sujo), ("INS02", "inse", r["inse"], sujo, "aceito", "nenhum")

    def inse_fora(self, r):
        sujo = arred(r["inse"] * 10, 1)
        return dict(r, inse=sujo), ("INS04", "inse", r["inse"], sujo, "quarentena", "remover")

    def inse_qtd_texto(self, r):
        v = r.get("quantidade_alunos_inse")
        if v is None:
            return None
        sujo = self.escolha([str(v), f"{v}.0"])
        return dict(r, quantidade_alunos_inse=sujo), ("G08", "quantidade_alunos_inse", v, sujo, "aceito", "nenhum")

    # ---- Docente ---------------------------------------------------------
    def doc_idade(self, r):
        sujo = self.escolha([0, 2, 150, 999, -5, "abc"])
        return dict(r, idade=sujo), ("DOC04", "idade", r["idade"], sujo, "aceito", "nulo:idade")

    def doc_escolaridade(self, r):
        v = r.get("escolaridade")
        if v not in ("3", "4"):
            return None
        sujo = self.escolha(["Superior Completo", "Ensino Superior Completo", "superior", "4.0", 4]) if v == "4" \
            else self.escolha(["Ensino Médio", "3.0", 3])
        return dict(r, escolaridade=sujo), ("DOC05", "escolaridade", v, sujo, "aceito", "nenhum")

    def doc_flags(self, r):
        cols = [c for c in E.DOCENTE_FLAGS if r.get(c) in (0, 1)]
        if not cols:
            return None
        cols = self.rng.choice(cols, size=min(len(cols), 1 + int(self.rng.integers(2))), replace=False)
        sujo = dict(r)
        for c in cols:
            sujo[c] = self.escolha(["Sim", "S", "true"]) if r[c] == 1 else self.escolha(["Não", "N", "false"])
        return sujo, ("DOC06", ",".join(cols), "0/1", "texto", "aceito", "nenhum")

    def doc_sem_id(self, r):
        return dict(r, id_docente=self.escolha([None, ""])), ("DOC07", "id_docente", r["id_docente"], None, "quarentena", "remover")

    def doc_contratacao(self, r):
        v = r.get("tipo_contratacao")
        texto = {"1": ["Concursado/efetivo/estável", "efetivo", 1], "2": ["Contrato temporário", "temporario", 2],
                 "3": ["Contrato terceirizado"], "4": ["Contrato CLT", "clt"]}.get(v)
        if not texto:
            return None
        sujo = self.escolha(texto)
        return dict(r, tipo_contratacao=sujo), ("DOC08", "tipo_contratacao", v, sujo, "aceito", "nenhum")

    def doc_etapa(self, r):
        v = r["etapa_ensino"]
        sujo = self.escolha([int(v), f"{v}.0", f" {v} "])
        return dict(r, etapa_ensino=sujo), ("DOC09", "etapa_ensino", v, sujo, "aceito", "nenhum")


def menu_defeitos(inj: Injetor) -> dict[str, list]:
    """(probabilidade-base por linha elegível, função). No máximo um defeito principal por linha."""
    return {
        "ideb": [(0.004, inj.id_ausente), (0.03, inj.id_sujo), (0.02, inj.ano_sujo), (0.02, inj.campo_extra),
                 (0.05, inj.ideb_caixa), (0.04, inj.ideb_suprimido), (0.05, inj.ideb_virgula), (0.01, inj.ideb_fora)],
        "censo_escola": [(0.004, inj.id_ausente), (0.03, inj.id_sujo), (0.02, inj.ano_sujo), (0.03, inj.campo_extra),
                         (0.03, inj.censo_uf), (0.10, inj.censo_flag_texto), (0.05, inj.censo_sentinela),
                         (0.10, inj.censo_categoria), (0.06, inj.censo_quantidade_texto), (0.03, inj.censo_municipio)],
        "indicadores": [(0.004, inj.id_ausente), (0.03, inj.id_sujo), (0.02, inj.ano_sujo), (0.02, inj.campo_extra),
                        (0.10, inj.ind_virgula), (0.05, inj.ind_percentual), (0.10, inj.ind_icg), (0.015, inj.ind_fora)],
        "inse": [(0.004, inj.id_ausente), (0.03, inj.id_sujo), (0.02, inj.ano_sujo), (0.025, inj.inse_nulo),
                 (0.08, inj.inse_virgula), (0.01, inj.inse_fora), (0.05, inj.inse_qtd_texto)],
        "docente": [(0.002, inj.id_ausente), (0.01, inj.id_sujo), (0.01, inj.ano_sujo), (0.01, inj.doc_idade),
                    (0.04, inj.doc_escolaridade), (0.04, inj.doc_flags), (0.003, inj.doc_sem_id),
                    (0.03, inj.doc_contratacao), (0.03, inj.doc_etapa)],
    }


def status_natural(tabela: str, r: dict) -> tuple[str, str | None]:
    """Destino das linhas limpas que já nascem fora do recorte (filtros da fase 1)."""
    if tabela == "ideb" and (r.get("ensino"), r.get("anos_escolares")) != (E.IDEB_ENSINO_ALVO, E.IDEB_ANOS_ESCOLARES_ALVO):
        return "descartado", "IDB01"
    if tabela == "ideb" and r.get("ideb") is None:
        return "descartado", "IDB03"
    if tabela == "docente":
        if r.get("etapa_ensino") is None:
            return "descartado", "DOC02"
        if r["etapa_ensino"] not in E.ETAPAS_ANOS_FINAIS:
            return "descartado", "DOC01"
    if tabela == "inse" and r.get("inse") is None:
        return "descartado", "INS01"
    return "aceito", None


# =============================================================================
# 5. Orquestração
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--semente", default=str(RAIZ / "semente"), help="pasta com os CSVs da semente real")
    ap.add_argument("--saida", default=str(RAIZ / "dados"), help="pasta de saída")
    ap.add_argument("--n-escolas", type=int, default=2000)
    ap.add_argument("--ano", type=int, default=2023, help="ano gravado nas escolas do fluxo")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--escala-defeitos", type=float, default=1.0, help="multiplica todas as taxas de defeito")
    ap.add_argument("--vinculos-p", type=float, default=0.175,
                    help="cada docente tem 1 + Binomial(2, p) turmas nos anos finais (média 1,35 com o padrão)")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    saida = Path(args.saida)
    for sub in ("bronze", "fluxo", "gabarito"):
        (saida / sub).mkdir(parents=True, exist_ok=True)

    print("Carregando semente...")
    sem = carregar(Path(args.semente))
    print(f"  {len(sem['gold'])} escolas reais de 2019 na semente")
    ids = amostrar_escolas(sem["gold"], args.n_escolas, rng)

    print(f"Gerando {len(ids)} escolas do fluxo a partir de escolas reais de 2019 (ano gravado {args.ano})...")
    fab = Fabrica(sem, args.ano, rng, args.vinculos_p)
    escolas = [fab.escola(sid, i) for i, sid in enumerate(ids)]

    # ---- injeção de defeitos -------------------------------------------------
    inj = Injetor(rng, args.escala_defeitos)
    menu = menu_defeitos(inj)
    sujas = {t: [] for t in E.TABELAS}            # linhas que vão para os arquivos
    meta = {t: [] for t in E.TABELAS}             # (escola_seq, tipo, status, codigo) por linha
    esperado = {t: [] for t in E.TABELAS}         # linhas limpas que devem sobreviver (gabarito)
    manifesto = []

    def emitir(tab, linha, seq, tipo, status, codigo):
        sujas[tab].append(linha)
        meta[tab].append((seq, tipo, status, codigo))
        return len(sujas[tab]) - 1

    for esc in escolas:
        for tab in E.TABELAS:
            for r in esc["linhas"][tab]:
                seq = r.pop("_escola_seq")
                st_nat, cod_nat = status_natural(tab, r)
                if st_nat != "aceito":
                    emitir(tab, r, seq, "normal", st_nat, cod_nat)
                    continue
                sujo, info = r, None
                for prob, fn in menu[tab]:
                    if inj.p(prob):
                        res = fn(r)
                        if res:
                            sujo, info = res
                            break
                # nulos extras no censo podem acompanhar outro defeito recuperável
                extra_info = None
                if tab == "censo_escola" and (info is None or info[5] == "nenhum") and inj.p(0.30):
                    res = inj.censo_nulos(sujo)
                    if res:
                        sujo, extra_info = res
                if tab == "indicadores" and (info is None or info[5] == "nenhum") and inj.p(0.7):
                    res = inj.ind_marcador(sujo)
                    if res:
                        sujo, extra_info = res
                status, codigo = (info[4], info[0]) if info else ("aceito", None)
                linha = emitir(tab, sujo, seq, "normal", status, codigo)
                for i in (info, extra_info):
                    if i:
                        manifesto.append({"tabela": tab, "linha": linha, "id_escola": r["id_escola"],
                                          "codigo": i[0], "coluna": i[1], "valor_limpo": i[2],
                                          "valor_sujo": i[3], "destino_esperado": i[4], "efeito": i[5]})
                efeito = info[5] if info else "nenhum"
                if efeito == "remover":
                    continue
                limpo = dict(r)
                if efeito.startswith("nulo:"):
                    limpo[efeito.split(":", 1)[1]] = None
                esperado[tab].append(limpo)
                # duplicatas (reenvio do mesmo registro) e duplicatas conflitantes
                if info is None and inj.p(0.015 if tab == "docente" else 0.02):
                    i2 = emitir(tab, dict(r), seq, "duplicata", "aceito", "G01")
                    manifesto.append({"tabela": tab, "linha": i2, "id_escola": r["id_escola"], "codigo": "G01",
                                      "coluna": "*", "valor_limpo": None, "valor_sujo": "reenvio",
                                      "destino_esperado": "aceito", "efeito": "nenhum"})
                if tab == "ideb" and info is None and inj.p(0.02):
                    conflito = dict(r, ideb=arred(max(0.1, r["ideb"] - rng.uniform(0.1, 0.6)), 1))
                    i2 = emitir(tab, conflito, seq, "duplicata", "aceito", "IDB06")
                    manifesto.append({"tabela": tab, "linha": i2, "id_escola": r["id_escola"], "codigo": "IDB06",
                                      "coluna": "ideb", "valor_limpo": r["ideb"], "valor_sujo": conflito["ideb"],
                                      "destino_esperado": "aceito", "efeito": "nenhum"})
                if tab == "inse" and info is None and inj.p(0.03) and r.get("quantidade_alunos_inse"):
                    conflito = dict(r, quantidade_alunos_inse=max(0, int(r["quantidade_alunos_inse"] * rng.uniform(0.3, 0.8))),
                                    inse=arred(r["inse"] + rng.choice([-0.3, 0.3]), 2))
                    i2 = emitir(tab, conflito, seq, "duplicata", "aceito", "INS03")
                    manifesto.append({"tabela": tab, "linha": i2, "id_escola": r["id_escola"], "codigo": "INS03",
                                      "coluna": "inse/quantidade_alunos_inse", "valor_limpo": r["inse"],
                                      "valor_sujo": conflito["inse"], "destino_esperado": "aceito", "efeito": "nenhum"})

    # mensagens com JSON corrompido
    total_msgs = sum(len(v) for v in sujas.values())
    malformadas = []
    for _ in range(int(rng.binomial(total_msgs, 0.002 * args.escala_defeitos))):
        tab = E.TABELAS[int(rng.integers(len(E.TABELAS)))]
        base = sujas[tab][int(rng.integers(len(sujas[tab])))]
        texto = json.dumps(_sem_nulos(base), ensure_ascii=False)
        corte = int(rng.integers(10, max(11, len(texto) // 2)))
        malformadas.append({"tabela": tab, "payload_bruto": texto[:corte],
                            "_escola_seq": int(rng.integers(len(escolas)))})

    # ---- gabarito -------------------------------------------------------------
    print("Calculando o gabarito (regras da fase 1, implementação de referência)...")
    gold = referencia_gold(esperado)

    # ---- escrita ---------------------------------------------------------------
    print("Gravando arquivos...")
    nomes = {t: f"{E.ARQUIVO_BRONZE[t]}.jsonl" for t in E.TABELAS}
    for t in E.TABELAS:
        with open(saida / "bronze" / nomes[t], "w", encoding="utf-8") as fh:
            for r in sujas[t]:
                fh.write(json.dumps(_sem_nulos(r), ensure_ascii=False, separators=(",", ":")) + "\n")
    with open(saida / "bronze" / "mensagens_malformadas.jsonl", "w", encoding="utf-8") as fh:
        for m in malformadas:
            fh.write(json.dumps({"tabela": m["tabela"], "payload_bruto": m["payload_bruto"]}, ensure_ascii=False) + "\n")

    eventos = _ordenar_eventos(meta, malformadas, rng)
    with open(saida / "fluxo" / "ordem_eventos.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["ordem", "tabela", "linha", "escola_seq", "tipo"])
        for i, ev in enumerate(eventos):
            w.writerow([i, *ev])

    with open(saida / "gabarito" / "status_esperado.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["tabela", "linha", "tipo", "status", "codigo"])
        for t in E.TABELAS:
            for i, (_, tipo, st, cod) in enumerate(meta[t]):
                w.writerow([t, i, tipo, st, cod or ""])
        for i, _ in enumerate(malformadas):
            w.writerow(["malformada", i, "malformada", "quarentena", "G06"])

    pd.DataFrame(manifesto).to_csv(saida / "gabarito" / "manifesto_defeitos.csv", index=False)
    gold.to_csv(saida / "gabarito" / "gold_esperada.csv", index=False)
    pd.DataFrame([{"id_escola_fluxo": e["id_escola"], "id_escola_semente": e["id_semente"],
                   "regiao": e["regiao"], "quintil_inse": e["quintil_inse"]} for e in escolas]) \
        .to_csv(saida / "gabarito" / "mapa_semente.csv", index=False)

    resumo = {
        "parametros": vars(args),
        "escolas_do_fluxo": len(escolas),
        "mensagens_por_tabela": {t: len(sujas[t]) for t in E.TABELAS} | {"malformadas": len(malformadas)},
        "status_esperado": {t: dict(Counter(m[2] for m in meta[t])) for t in E.TABELAS}
        | {"malformadas": {"quarentena": len(malformadas)}},
        "codigos_injetados": dict(Counter(m["codigo"] for m in manifesto)),
        "linhas_gold_esperadas": len(gold),
        "distribuicao_gold_por_regiao": gold["sigla_uf"].map(E.REGIAO_UF).value_counts().to_dict(),
        "ideb_medio_esperado": round(float(gold["ideb"].mean()), 3),
    }
    (saida / "gabarito" / "resumo_esperado.json").write_text(json.dumps(resumo, ensure_ascii=False, indent=2),
                                                             encoding="utf-8")
    print(json.dumps({k: resumo[k] for k in ("escolas_do_fluxo", "mensagens_por_tabela", "linhas_gold_esperadas")},
                     ensure_ascii=False, indent=2))


def _sem_nulos(r: dict) -> dict:
    """Campos nulos são omitidos do JSON (convenção comum em eventos; ausência = nulo)."""
    out = {}
    for k, v in r.items():
        if k.startswith("_") or v is None:
            continue
        if isinstance(v, (np.integer,)):
            v = int(v)
        elif isinstance(v, (np.floating,)):
            v = float(v)
        out[k] = v
    return out


def _ordenar_eventos(meta, malformadas, rng):
    """Ordem de envio: escola a escola, tabelas embaralhadas dentro da escola.

    Duplicatas chegam logo depois ou bem mais tarde (reenvio). Cerca de 3% das escolas têm
    uma linha de docente atrasada várias escolas depois: ela chega quando a gold da escola já
    existe e força uma atualização (MERGE), como um dado tardio real.
    """
    por_escola = defaultdict(list)
    atrasados = []
    for t in E.TABELAS:
        for i, (seq, tipo, st, _) in enumerate(meta[t]):
            ev = (t, i, seq, tipo)
            if tipo == "duplicata" and rng.random() < 0.5:
                atrasados.append((seq + int(rng.integers(1, 40)), ev))
            else:
                por_escola[seq].append(ev)
    escolas_com_atraso = set()
    for seq, evs in por_escola.items():
        if rng.random() < 0.03:
            cand = [k for k, ev in enumerate(evs) if ev[0] == "docente" and meta["docente"][ev[1]][2] == "aceito"
                    and ev[3] == "normal"]
            if cand:
                k = cand[int(rng.integers(len(cand)))]
                ev = evs.pop(k)
                atrasados.append((seq + int(rng.integers(15, 60)), (ev[0], ev[1], ev[2], "atrasado")))
                escolas_com_atraso.add(seq)
    for j, m in enumerate(malformadas):
        por_escola[m["_escola_seq"]].append(("malformada", j, m["_escola_seq"], "malformada"))
    for alvo, ev in atrasados:
        por_escola[alvo].append(ev)
    ordem = []
    for seq in sorted(por_escola):
        evs = por_escola[seq]
        normais = [e for e in evs if e[3] in ("normal", "malformada")]
        tardios = [e for e in evs if e[3] not in ("normal", "malformada")]
        rng.shuffle(normais)
        ordem += normais + tardios
    return ordem


if __name__ == "__main__":
    main()
