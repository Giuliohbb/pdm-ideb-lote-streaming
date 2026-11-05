"""Regras de limpeza linha a linha (sem estado) aplicadas a cada mensagem do Pub/Sub.

Cada função `limpar_<tabela>` recebe o registro bruto (dict vindo do JSON) e devolve um
`Resultado` com um de três destinos:

  * "aceito"      -> registro limpo, gravado na silver de streaming da tabela;
  * "descartado"  -> regra de negócio herdada da fase 1 (ex.: etapa fora do recorte,
                     IDEB suprimido, INSE nulo). Não é erro, é filtro;
  * "quarentena"  -> erro de qualidade que impede o uso do registro (chave ausente,
                     valor fora da faixa, JSON malformado).

As regras que só fazem sentido sobre um conjunto de linhas (deduplicação, agregação dos
docentes e junção das cinco fontes) não estão aqui: elas exigem estado e rodam na
consolidação em micro-lotes (construir_gold.py).

Os códigos das regras (G02, IDB04, CEN01...) são os mesmos do catálogo de defeitos.
"""
from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable

import esquema as E

ACEITO, DESCARTADO, QUARENTENA = "aceito", "descartado", "quarentena"

MARCADORES_NULO = {"", "-", "--", "*", "na", "n/a", "nan", "null", "none", "nd", "s/i", "x"}
_RE_MILHAR = re.compile(r"^-?\d{1,3}(\.\d{3})+$")


@dataclass
class Resultado:
    status: str
    registro: dict | None = None
    regras: list[str] = field(default_factory=list)
    motivo: str | None = None
    id_escola: str | None = None


class _Ctx:
    """Acumula as regras aplicadas a um registro, contando repetições por código."""

    def __init__(self) -> None:
        self._contagem: dict[str, int] = {}

    def add(self, codigo: str, n: int = 1) -> None:
        self._contagem[codigo] = self._contagem.get(codigo, 0) + n

    def lista(self) -> list[str]:
        return [c if n == 1 else f"{c}x{n}" for c, n in sorted(self._contagem.items())]


class _Quarentena(Exception):
    def __init__(self, codigo: str, motivo: str):
        super().__init__(motivo)
        self.codigo, self.motivo = codigo, motivo


class _Descarte(Exception):
    def __init__(self, codigo: str, motivo: str):
        super().__init__(motivo)
        self.codigo, self.motivo = codigo, motivo


# ---------------------------------------------------------------------------
# Conversores tolerantes. Cada um devolve (valor, tipo_de_correcao | None).
# ---------------------------------------------------------------------------
def _sem_acento(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _norm_texto(v: Any) -> str:
    return re.sub(r"\s+", " ", _sem_acento(str(v)).strip().lower())


def _eh_nulo(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return isinstance(v, str) and v.strip().lower() in MARCADORES_NULO


def para_float(v: Any) -> tuple[float | None, str | None]:
    """Número decimal. Correções: 'marcador' ("-", "--", "*"), 'virgula', 'percentual', 'texto'."""
    if v is None:
        return None, None
    if isinstance(v, bool):
        return float(v), "texto"
    if isinstance(v, (int, float)):
        return (None, None) if (isinstance(v, float) and math.isnan(v)) else (float(v), None)
    s = str(v).strip()
    if s.lower() in MARCADORES_NULO:
        return None, "marcador"
    tipo = "texto"
    if s.endswith("%"):
        s, tipo = s[:-1].strip(), "percentual"
    if "," in s:
        s = s.replace(".", "").replace(",", ".") if "." in s else s.replace(",", ".")
        tipo = "percentual" if tipo == "percentual" else "virgula"
    try:
        return float(s), tipo
    except ValueError:
        return None, "invalido"


def para_int(v: Any) -> tuple[int | None, str | None]:
    """Inteiro. Correções: 'marcador', 'milhar' ("1.234"), 'texto' ("12", "12.0", 12.0)."""
    if v is None:
        return None, None
    if isinstance(v, bool):
        return int(v), "texto"
    if isinstance(v, int):
        return v, None
    if isinstance(v, float):
        if math.isnan(v):
            return None, None
        return (int(v), "texto") if v.is_integer() else (None, "invalido")
    s = str(v).strip()
    if s.lower() in MARCADORES_NULO:
        return None, "marcador"
    if _RE_MILHAR.match(s):
        return int(s.replace(".", "")), "milhar"
    try:
        f = float(s.replace(",", "."))
    except ValueError:
        return None, "invalido"
    return (int(f), "texto") if f.is_integer() else (None, "invalido")


_VERDADEIRO = {"1", "1.0", "sim", "s", "true", "t", "yes", "y", "verdadeiro", "possui"}
_FALSO = {"0", "0.0", "nao", "n", "false", "f", "no", "falso", "nao possui"}


def para_flag(v: Any) -> tuple[int | None, str | None]:
    """Flag 0/1. Correções: 'texto' ("Sim", "true", "S", "1.0"), 'invalido'."""
    if v is None:
        return None, None
    if isinstance(v, bool):
        return int(v), "texto"
    if isinstance(v, int) and v in (0, 1):
        return v, None
    if isinstance(v, float) and v in (0.0, 1.0):
        return int(v), "texto"
    if isinstance(v, (int, float)):
        return None, "invalido"
    s = _norm_texto(v)
    if s in MARCADORES_NULO:
        return None, "marcador"
    if s in _VERDADEIRO:
        return 1, "texto"
    if s in _FALSO:
        return 0, "texto"
    return None, "invalido"


def _codigo_texto(v: Any) -> tuple[str | None, str | None]:
    """Código categórico numérico como texto: 19, 19.0, "19.0", " 19 " -> "19"."""
    if _eh_nulo(v):
        return None, None
    if isinstance(v, bool):
        return None, "invalido"
    if isinstance(v, (int, float)):
        return (str(int(v)), "numero") if float(v).is_integer() else (None, "invalido")
    s = str(v).strip()
    if re.fullmatch(r"\d+", s):
        return s, (None if s == str(v) else "espacos")
    try:
        f = float(s)
        return (str(int(f)), "numero") if f.is_integer() else (None, "invalido")
    except ValueError:
        return None, "invalido"


# ---------------------------------------------------------------------------
# Regras genéricas (chave e identificação)
# ---------------------------------------------------------------------------
def _id_escola(reg: dict, ctx: _Ctx) -> str:
    v = reg.get("id_escola")
    if _eh_nulo(v):
        raise _Quarentena("G03", "id_escola ausente")
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    bruto = str(v)
    digitos = re.sub(r"\D", "", bruto)
    if len(digitos) != 8:
        raise _Quarentena("G03", f"id_escola invalido: {bruto!r}")
    if digitos != bruto or not isinstance(reg.get("id_escola"), str):
        ctx.add("G02")
    return digitos


def _ano(reg: dict, ctx: _Ctx) -> int:
    v, corr = para_int(reg.get("ano"))
    if v is None:
        raise _Quarentena("G04", f"ano ausente ou invalido: {reg.get('ano')!r}")
    if not (E.FAIXA_ANO[0] <= v <= E.FAIXA_ANO[1]):
        raise _Quarentena("G04", f"ano fora da faixa: {v}")
    if corr:
        ctx.add("G04")
    return v


def _campos_desconhecidos(reg: dict, conhecidos: set[str], ctx: _Ctx) -> None:
    extras = [k for k in reg if k not in conhecidos]
    if extras:
        ctx.add("G07", len(extras))


# ---------------------------------------------------------------------------
# IDEB
# ---------------------------------------------------------------------------
def limpar_ideb(reg: dict, ctx: _Ctx, colunas_bronze: set[str]) -> dict:
    _campos_desconhecidos(reg, colunas_bronze, ctx)
    id_escola = _id_escola(reg, ctx)
    ano = _ano(reg, ctx)

    ensino_bruto, anos_bruto = reg.get("ensino"), reg.get("anos_escolares")
    ensino = _norm_texto(ensino_bruto) if ensino_bruto is not None else ""
    anos = _norm_texto(anos_bruto) if anos_bruto is not None else ""
    if (ensino_bruto is not None and ensino != ensino_bruto) or \
       (anos_bruto is not None and anos != anos_bruto):
        ctx.add("IDB02")
    if ensino != E.IDEB_ENSINO_ALVO or anos != E.IDEB_ANOS_ESCOLARES_ALVO:
        raise _Descarte("IDB01", f"etapa fora do recorte: {ensino} / {anos}")

    ideb, corr = para_float(reg.get("ideb"))
    if corr == "invalido":
        raise _Quarentena("IDB05", f"ideb nao numerico: {reg.get('ideb')!r}")
    if ideb is None:
        raise _Descarte("IDB03", "ideb nulo/suprimido pelo INEP")
    if corr == "virgula":
        ctx.add("IDB04")
    elif corr:
        ctx.add("G08")
    if not (E.FAIXA_IDEB[0] <= ideb <= E.FAIXA_IDEB[1]):
        raise _Quarentena("IDB05", f"ideb fora de [0, 10]: {ideb}")
    return {"id_escola": id_escola, "ano": ano, "ideb": round(ideb, 1)}


# ---------------------------------------------------------------------------
# Censo Escola
# ---------------------------------------------------------------------------
def _categoria(valor: Any, alias: dict[str, str], coluna: str, ctx: _Ctx) -> str:
    if _eh_nulo(valor):
        raise _Quarentena("CEN04", f"{coluna} ausente")
    chave = _norm_texto(valor)
    if re.fullmatch(r"\d+\.0+", chave):
        chave = chave.split(".")[0]
    if chave not in alias:
        raise _Quarentena("CEN04", f"{coluna} fora do dominio: {valor!r}")
    codigo = alias[chave]
    if codigo != valor:
        ctx.add("CEN04")
    return codigo


def limpar_censo_escola(reg: dict, ctx: _Ctx, colunas_bronze: set[str]) -> dict:
    _campos_desconhecidos(reg, colunas_bronze, ctx)
    out: dict[str, Any] = {"id_escola": _id_escola(reg, ctx), "ano": _ano(reg, ctx)}

    uf = reg.get("sigla_uf")
    uf_norm = str(uf).strip().upper() if not _eh_nulo(uf) else None
    if uf_norm not in E.UFS:
        raise _Quarentena("G05", f"sigla_uf invalida: {uf!r}")
    if uf_norm != uf:
        ctx.add("G05")
    out["sigla_uf"] = uf_norm

    mun = reg.get("id_municipio")
    if _eh_nulo(mun):
        raise _Quarentena("CEN06", "id_municipio ausente")
    if isinstance(mun, float) and mun.is_integer():
        mun = int(mun)
    mun_txt = str(mun).strip()
    mun_txt = mun_txt[:-2] if mun_txt.endswith(".0") else mun_txt
    mun_dig = re.sub(r"\D", "", mun_txt)
    if len(mun_dig) != 7 or mun_dig[:2] not in E.CODIGO_UF:
        raise _Quarentena("CEN06", f"id_municipio invalido: {mun!r}")
    if mun_dig != reg.get("id_municipio"):
        ctx.add("CEN06")
    out["id_municipio"] = mun_dig

    out["rede"] = _categoria(reg.get("rede"), E.REDE_ALIAS, "rede", ctx)
    out["tipo_localizacao"] = _categoria(reg.get("tipo_localizacao"), E.LOCALIZACAO_ALIAS,
                                         "tipo_localizacao", ctx)
    dif = reg.get("tipo_localizacao_diferenciada")
    if _eh_nulo(dif):
        out["tipo_localizacao_diferenciada"] = None  # fase 1 manteve nulo nesta coluna
    else:
        cod, _ = _codigo_texto(dif)
        cod = f"{cod}.0" if cod is not None else None
        if cod not in E.LOCALIZACAO_DIFERENCIADA_VALIDOS:
            raise _Quarentena("CEN04", f"tipo_localizacao_diferenciada fora do dominio: {dif!r}")
        if cod != dif:
            ctx.add("CEN04")
        out["tipo_localizacao_diferenciada"] = cod

    nulos = 0
    for bronze, silver, _ in E.CENSO_ATRIBUTOS:
        bruto = reg.get(bronze)
        como_int, _ = para_int(bruto) if not isinstance(bruto, bool) else (None, None)
        if como_int is not None and como_int < 0:
            ctx.add("CEN03")                 # sentinela negativa (-1 = "nao informado")
            v = None
        elif E.censo_eh_quantidade(bronze):
            v, corr = para_int(bruto)
            if corr in ("milhar", "texto", "invalido"):
                ctx.add("CEN05")
        else:
            v, corr = para_flag(bruto)
            if corr in ("texto", "invalido"):
                ctx.add("CEN02")
        if v is None:
            nulos += 1
            v = 0                            # regra da fase 1: COALESCE(coluna, 0)
        out[silver] = v
    if nulos:
        ctx.add("CEN01", nulos)
    return out


# ---------------------------------------------------------------------------
# Indicadores educacionais
# ---------------------------------------------------------------------------
def _icg(valor: Any, ctx: _Ctx) -> str | None:
    if _eh_nulo(valor):
        return None
    s = _norm_texto(valor).replace("_", " ").replace("-", " ")
    m = re.fullmatch(r"(?:nivel\s*)?(\d)(?:\.0)?", s)
    if not m:
        ctx.add("IND04")
        return None
    cod = f"nivel {m.group(1)}"
    if cod not in E.ICG_VALIDOS:
        ctx.add("IND05")
        return None
    if cod != valor:
        ctx.add("IND04")
    return cod


def limpar_indicadores(reg: dict, ctx: _Ctx, colunas_bronze: set[str]) -> dict:
    _campos_desconhecidos(reg, colunas_bronze, ctx)
    out: dict[str, Any] = {"id_escola": _id_escola(reg, ctx), "ano": _ano(reg, ctx)}
    for col in E.INDICADORES_NUMERICOS:
        v, corr = para_float(reg.get(col))
        if corr == "marcador":
            ctx.add("IND01")
        elif corr == "virgula":
            ctx.add("IND02")
        elif corr == "percentual":
            ctx.add("IND03")
        elif corr == "texto":
            ctx.add("G08")
        elif corr == "invalido":
            ctx.add("IND05")
        lo, hi = E.FAIXA_INDICADOR[col]
        if v is not None and not (lo <= v <= hi):
            ctx.add("IND05")
            v = None                          # sem COALESCE, como na fase 1
        out[col] = v
    out[E.INDICADOR_ICG] = _icg(reg.get(E.INDICADOR_ICG), ctx)
    return out


# ---------------------------------------------------------------------------
# INSE
# ---------------------------------------------------------------------------
def limpar_inse(reg: dict, ctx: _Ctx, colunas_bronze: set[str]) -> dict:
    _campos_desconhecidos(reg, colunas_bronze, ctx)
    id_escola, ano = _id_escola(reg, ctx), _ano(reg, ctx)
    inse, corr = para_float(reg.get("inse"))
    if corr == "invalido":
        raise _Quarentena("INS04", f"inse nao numerico: {reg.get('inse')!r}")
    if inse is None:
        raise _Descarte("INS01", "inse nulo")
    if corr == "virgula":
        ctx.add("INS02")
    elif corr:
        ctx.add("G08")
    if not (E.FAIXA_INSE[0] <= inse <= E.FAIXA_INSE[1]):
        raise _Quarentena("INS04", f"inse fora da escala: {inse}")
    qtd, corr_q = para_int(reg.get("quantidade_alunos_inse"))
    if corr_q in ("texto", "milhar"):
        ctx.add("G08")
    if qtd is not None and qtd < 0:
        qtd = None
    return {"id_escola": id_escola, "ano": ano, "inse": round(inse, 2),
            "quantidade_alunos_inse": qtd}


# ---------------------------------------------------------------------------
# Docente (grão docente x turma)
# ---------------------------------------------------------------------------
def _escolaridade(v: Any, ctx: _Ctx) -> str | None:
    if _eh_nulo(v):
        return None
    cod, corr = _codigo_texto(v)
    if cod is not None:
        if corr:
            ctx.add("DOC05")
        return cod
    s = _norm_texto(v)
    if s in E.ESCOLARIDADE_ALIAS:
        ctx.add("DOC05")
        return E.ESCOLARIDADE_ALIAS[s]
    ctx.add("DOC05")
    return None


def _contratacao(v: Any, ctx: _Ctx) -> str | None:
    if _eh_nulo(v):
        return None
    cod, corr = _codigo_texto(v)
    if cod is not None:
        if corr:
            ctx.add("DOC08")
        return cod
    s = _norm_texto(v)
    ctx.add("DOC08")
    return E.CONTRATACAO_ALIAS.get(s)


def limpar_docente(reg: dict, ctx: _Ctx, colunas_bronze: set[str]) -> dict:
    _campos_desconhecidos(reg, colunas_bronze, ctx)
    id_escola, ano = _id_escola(reg, ctx), _ano(reg, ctx)

    id_docente = reg.get("id_docente")
    if _eh_nulo(id_docente):
        raise _Quarentena("DOC07", "id_docente ausente")
    id_docente = str(id_docente).strip().upper()

    etapa, corr = _codigo_texto(reg.get("etapa_ensino"))
    if corr in ("numero", "espacos"):
        ctx.add("DOC09")
    if etapa is None:
        raise _Descarte("DOC02", "etapa_ensino nula")
    if etapa not in E.ETAPAS_ANOS_FINAIS:
        raise _Descarte("DOC01", f"etapa {etapa} fora dos anos finais")

    id_turma, _ = _codigo_texto(reg.get("id_turma"))

    idade, corr_i = para_int(reg.get("idade"))
    if corr_i == "texto":
        ctx.add("G08")
    if corr_i == "invalido" or (idade is not None and not
                                (E.FAIXA_IDADE_DOCENTE[0] <= idade <= E.FAIXA_IDADE_DOCENTE[1])):
        ctx.add("DOC04")
        idade = None

    out = {
        "id_escola": id_escola, "ano": ano, "id_docente": id_docente, "id_turma": id_turma,
        "etapa_ensino": etapa, "idade": idade,
        "escolaridade": _escolaridade(reg.get("escolaridade"), ctx),
        "tipo_contratacao": _contratacao(reg.get("tipo_contratacao"), ctx),
    }
    for col in E.DOCENTE_FLAGS:
        v, c = para_flag(reg.get(col))
        if c in ("texto", "invalido"):
            ctx.add("DOC06")
        out[col] = v
    td, c = para_int(reg.get("tipo_docente"))
    if c:
        ctx.add("G08")
    out["tipo_docente"] = td
    return out


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
_LIMPADORES: dict[str, Callable[[dict, _Ctx, set[str]], dict]] = {
    "ideb": limpar_ideb,
    "censo_escola": limpar_censo_escola,
    "indicadores": limpar_indicadores,
    "inse": limpar_inse,
    "docente": limpar_docente,
}


def limpar(tabela: str, registro: Any, colunas_bronze: dict[str, set[str]] | None = None) -> Resultado:
    """Aplica as regras da tabela e devolve o destino do registro.

    `colunas_bronze` (opcional) informa as colunas conhecidas de cada bronze para detectar
    campos inesperados (G07). Sem ele, a checagem de campos extras é ignorada.
    """
    ctx = _Ctx()
    if tabela not in _LIMPADORES:
        return Resultado(QUARENTENA, None, ["G06"], f"tabela desconhecida: {tabela!r}")
    if not isinstance(registro, dict):
        return Resultado(QUARENTENA, None, ["G06"], "payload nao e um objeto JSON")
    conhecidas = (colunas_bronze or {}).get(tabela) or set(registro)
    id_bruto = registro.get("id_escola")
    try:
        limpo = _LIMPADORES[tabela](registro, ctx, conhecidas)
        return Resultado(ACEITO, limpo, ctx.lista(), None, limpo.get("id_escola"))
    except _Descarte as d:
        ctx.add(d.codigo)
        return Resultado(DESCARTADO, None, ctx.lista(), d.motivo, _id_seguro(id_bruto))
    except _Quarentena as q:
        ctx.add(q.codigo)
        return Resultado(QUARENTENA, None, ctx.lista(), q.motivo, _id_seguro(id_bruto))


def _id_seguro(v: Any) -> str | None:
    if _eh_nulo(v):
        return None
    d = re.sub(r"\D", "", str(int(v)) if isinstance(v, float) and v.is_integer() else str(v))
    return d or None
