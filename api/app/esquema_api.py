"""Contrato de entrada e saída da API e preparação das instâncias para o modelo.

A preparação segue as mesmas convenções do pipeline de limpeza e do BigQuery ML, para que a
predição servida pela API seja a mesma do ML.PREDICT (consistência entre treino e serviço):

  * categorias são normalizadas para o formato da gold ("estadual" vira "2", "0" vira "0.0" na
    localização diferenciada, "Nível 3" vira "nivel 3", UF em maiúsculas, município com 7 dígitos);
  * número nulo é preenchido com a média de treino, que é o que o ML.PREDICT faz;
  * texto nulo vira categoria ausente, que não tem peso no modelo (contribui com zero);
  * categoria que não apareceu no treino também contribui com zero, e a resposta avisa.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, create_model

from .features import FEATURES

# Mesmos domínios de pipeline/esquema.py
REDE_ALIAS = {
    "1": "1", "federal": "1", "2": "2", "estadual": "2", "3": "3", "municipal": "3",
    "4": "4", "privada": "4", "particular": "4",
}
LOCALIZACAO_ALIAS = {"1": "1", "urbana": "1", "urbano": "1", "2": "2", "rural": "2"}

DESCRICOES = {
    "ano": "Ano de referência (o modelo foi treinado com 2013 e 2015).",
    "sigla_uf": "UF da escola, por exemplo GO.",
    "id_municipio": "Código IBGE do município, 7 dígitos.",
    "rede": "1 federal, 2 estadual, 3 municipal, 4 privada (aceita o nome por extenso).",
    "tipo_localizacao": "1 urbana, 2 rural.",
    "tipo_localizacao_diferenciada": "Código do Censo no formato da gold: '0.0' a '7.0'.",
    "icg_nivel_complexidade_gestao_escola": "Nível de complexidade de gestão: 'nivel 1' a 'nivel 6'.",
    "inse": "Indicador de nível socioeconômico médio da escola.",
}

# Uma escola do fluxo de streaming (linha da gold esperada), usada como exemplo na página /docs
_ARQ_EXEMPLO = Path(__file__).with_name("exemplo_escola.json")
EXEMPLO: dict[str, Any] = json.loads(_ARQ_EXEMPLO.read_text(encoding="utf-8")) if _ARQ_EXEMPLO.exists() else {}


class ErroEntrada(ValueError):
    """Entrada que passou na validação de tipos, mas não pode ser preparada para o modelo."""


def _tipo_python(tipo_bq: str):
    return {"INT64": int, "FLOAT64": float, "STRING": str}[tipo_bq]


def _campos() -> dict:
    campos: dict[str, Any] = {
        "id_escola": (Optional[str], Field(None, description="Opcional. Identificador devolvido na resposta.")),
        "ideb": (Optional[float], Field(None, description="Opcional. IDEB observado; se vier, a resposta traz o erro.")),
    }
    for nome, tipo in FEATURES:
        campos[nome] = (Optional[_tipo_python(tipo)],
                        Field(..., description=DESCRICOES.get(nome, f"Feature {tipo} da gold_ideb_features.")))
    return campos


Escola = create_model(
    "Escola",
    __config__=ConfigDict(extra="forbid", coerce_numbers_to_str=True),
    **_campos(),
)


class PedidoPredicao(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": [{"instancias": [EXEMPLO]}]} if EXEMPLO else None)
    instancias: list[Escola] = Field(..., min_length=1, max_length=1000,  # type: ignore[valid-type]
                                     description="De 1 a 1000 escolas, cada uma com as 85 features.")


class Predicao(BaseModel):
    indice: int
    id_escola: Optional[str] = None
    ideb_previsto: float
    ideb_informado: Optional[float] = None
    erro_absoluto: Optional[float] = None
    avisos: list[str] = []


class InfoModelo(BaseModel):
    nome: str
    versao: str
    backend: str


class RespostaPredicao(BaseModel):
    modelo: InfoModelo
    quantidade: int
    predicoes: list[Predicao]
    latencia_ms: float


# ---------------------------------------------------------------------------
# Normalização das categorias (mesmas convenções do serviço de limpeza)
# ---------------------------------------------------------------------------
def _sem_acento(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _texto(v: Any) -> str:
    return re.sub(r"\s+", " ", _sem_acento(str(v)).strip().lower())


def normalizar_categoria(nome: str, valor: Any) -> Optional[str]:
    if valor is None or str(valor).strip() == "":
        return None
    bruto = str(valor).strip()
    if nome == "sigla_uf":
        return bruto.upper()
    if nome == "id_municipio":
        sem_decimal = bruto[:-2] if bruto.endswith(".0") else bruto
        return re.sub(r"\D", "", sem_decimal) or bruto
    if nome == "rede":
        return REDE_ALIAS.get(_texto(bruto), bruto)
    if nome == "tipo_localizacao":
        return LOCALIZACAO_ALIAS.get(_texto(bruto), bruto)
    if nome == "tipo_localizacao_diferenciada":
        try:
            return f"{int(float(bruto))}.0"
        except ValueError:
            return bruto
    if nome == "icg_nivel_complexidade_gestao_escola":
        m = re.fullmatch(r"(?:nivel\s*)?(\d)(?:\.0)?", _texto(bruto).replace("_", " ").replace("-", " "))
        return f"nivel {m.group(1)}" if m else bruto
    return bruto


def preparar(instancias: list, metadados) -> tuple[list[dict], list[list[str]]]:
    """Converte as instâncias validadas em linhas prontas para o modelo, com os avisos de cada uma."""
    linhas, avisos = [], []
    for i, inst in enumerate(instancias):
        dados = inst.model_dump() if hasattr(inst, "model_dump") else dict(inst)
        linha: dict[str, Any] = {}
        av: list[str] = []
        for nome, tipo in FEATURES:
            valor = dados.get(nome)
            if tipo == "STRING":
                cat = normalizar_categoria(nome, valor)
                if cat is None:
                    av.append(f"{nome}: nulo; tratado como categoria ausente (peso zero)")
                    cat = ""
                elif metadados is not None and not metadados.categoria_conhecida(nome, cat):
                    av.append(f"{nome}: categoria '{cat}' não apareceu no treino; contribui com peso zero")
                linha[nome] = cat
            else:
                if valor is None:
                    media = metadados.media(nome) if metadados is not None else None
                    if media is None:
                        raise ErroEntrada(f"instância {i}: {nome} é nulo e não há média de treino para preencher")
                    valor = media
                    av.append(f"{nome}: nulo; preenchido com a média de treino ({media:.6g}), como no ML.PREDICT")
                linha[nome] = valor
        linhas.append(linha)
        avisos.append(av)
    return linhas, avisos
