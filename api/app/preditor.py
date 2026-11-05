"""Carregamento do modelo e execução da inferência.

Dois backends, escolhidos pela variável de ambiente BACKEND:

  savedmodel (padrão)  A API baixa do Cloud Storage o TensorFlow SavedModel que o BigQuery ML
                       exportou (o mesmo artefato registrado no Vertex AI Model Registry) e prevê
                       em memória. Escala a zero junto com o Cloud Run.
  vertex               A API repassa as instâncias a um Endpoint do Vertex AI onde esse artefato
                       foi implantado (infra/endpoint_vertex.sh) e devolve a resposta.

O SavedModel é lido pela própria assinatura (serving_default): a API descobre o nome, o tipo e o
formato de cada entrada e converte os valores para eles. Assim ela não depende de detalhes internos
da exportação do BigQuery ML, e falha logo na partida, com mensagem clara, se o modelo carregado
não esperar exatamente as 85 features da gold.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

from .features import FEATURES, NOMES, TIPOS

log = logging.getLogger("api_ideb")


class ErroBackend(RuntimeError):
    """Falha ao executar a inferência (modelo ou endpoint indisponível)."""


# ---------------------------------------------------------------------------
# Artefato: download do Cloud Storage e metadados
# ---------------------------------------------------------------------------
def baixar_diretorio(uri: str, destino: Path) -> Path:
    """Copia gs://bucket/prefixo/ para uma pasta local. Caminhos locais são usados como estão."""
    if not uri.startswith("gs://"):
        return Path(uri)
    from google.cloud import storage

    bucket, _, prefixo = uri[len("gs://"):].partition("/")
    prefixo = prefixo.rstrip("/") + "/"
    cliente = storage.Client()
    n = 0
    for blob in cliente.list_blobs(bucket, prefix=prefixo):
        if blob.name.endswith("/"):
            continue
        alvo = destino / blob.name[len(prefixo):]
        alvo.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(alvo))
        n += 1
    if n == 0:
        raise FileNotFoundError(f"nenhum arquivo encontrado em {uri}")
    log.info("artefato baixado", extra={"campos": {"uri": uri, "arquivos": n}})
    return destino


class Metadados:
    """Leitura do metadados_modelo.json gravado pelo infra/registrar_modelo.sh."""

    def __init__(self, dados: dict):
        self.dados = dados
        self._medias = {f["nome"]: f.get("media_treino") for f in dados.get("features", [])
                        if f.get("tipo") == "numerica"}
        self._categorias = {f["nome"]: set(f.get("categorias_no_treino") or [])
                            for f in dados.get("features", []) if f.get("tipo") == "categorica"}

    @classmethod
    def ler(cls, caminho: Path) -> Optional["Metadados"]:
        if not caminho.exists():
            log.warning("metadados_modelo.json não encontrado; nulos não poderão ser preenchidos")
            return None
        return cls(json.loads(caminho.read_text(encoding="utf-8")))

    def media(self, nome: str) -> Optional[float]:
        return self._medias.get(nome)

    def categoria_conhecida(self, nome: str, valor: str) -> bool:
        cats = self._categorias.get(nome)
        return True if not cats else valor in cats

    @property
    def versao(self) -> str:
        return str(self.dados.get("versao", "desconhecida"))

    @property
    def nome(self) -> str:
        return str(self.dados.get("modelo", "modelo_ideb"))

    def resumo(self) -> dict:
        d = {k: v for k, v in self.dados.items() if k != "features"}
        d["features"] = [{k: v for k, v in f.items() if k != "categorias_no_treino"}
                         | ({"quantidade_categorias": len(f.get("categorias_no_treino") or [])}
                            if f.get("tipo") == "categorica" else {})
                         for f in self.dados.get("features", [])]
        return d


def _escolher_saida(chaves: list[str]) -> str:
    if "predicted_ideb" in chaves:
        return "predicted_ideb"
    previstas = [c for c in chaves if c.startswith("predicted")]
    if previstas:
        return previstas[0]
    if len(chaves) == 1:
        return chaves[0]
    raise ErroBackend(f"não sei qual saída do modelo usar entre {sorted(chaves)}")


def _primeiro_numero(p: Any) -> float:
    """Extrai o número de uma predição no formato do TF Serving (número, lista ou dicionário)."""
    if isinstance(p, dict):
        return _primeiro_numero(p[_escolher_saida(list(p))])
    if isinstance(p, (list, tuple)):
        return _primeiro_numero(p[0])
    return float(p)


# ---------------------------------------------------------------------------
# Backend 1: SavedModel carregado na própria API
# ---------------------------------------------------------------------------
class PreditorSavedModel:
    backend = "savedmodel"

    def __init__(self, dir_modelo: Path):
        import tensorflow as tf  # importado aqui para o backend vertex não pagar o custo

        self.tf = tf
        inicio = time.perf_counter()
        self.modelo = tf.saved_model.load(str(dir_modelo))
        if "serving_default" not in self.modelo.signatures:
            raise ErroBackend(f"o SavedModel em {dir_modelo} não tem a assinatura serving_default")
        self.fn = self.modelo.signatures["serving_default"]
        self.entradas = dict(self.fn.structured_input_signature[1])
        self.saida = _escolher_saida(list(self.fn.structured_outputs))
        nomes = set(self.entradas)
        if nomes == set(NOMES):
            self.modo = "features"
        elif len(nomes) == 1 and next(iter(self.entradas.values())).dtype == tf.string:
            self.modo = "tf_example"   # assinatura que recebe tf.Example serializado
        else:
            faltando, sobrando = sorted(set(NOMES) - nomes), sorted(nomes - set(NOMES))
            raise ErroBackend(f"o modelo não espera as 85 features da gold. Faltando: {faltando[:10]} "
                              f"Sobrando: {sobrando[:10]}")
        log.info("SavedModel carregado", extra={"campos": {
            "diretorio": str(dir_modelo), "modo": self.modo, "saida": self.saida,
            "segundos": round(time.perf_counter() - inicio, 2)}})

    def assinatura(self) -> dict:
        return {"modo": self.modo, "saida": self.saida,
                "entradas": {n: {"dtype": s.dtype.name, "shape": str(s.shape)}
                             for n, s in sorted(self.entradas.items())}}

    def _tensor(self, valores: list, spec):
        tf = self.tf
        if spec.dtype == tf.string:
            t = tf.constant([v if isinstance(v, bytes) else str(v) for v in valores], dtype=tf.string)
        elif spec.dtype == tf.bool:
            t = tf.constant([bool(v) for v in valores], dtype=tf.bool)
        elif spec.dtype.is_integer:
            t = tf.constant([int(round(float(v))) for v in valores], dtype=spec.dtype)
        else:
            t = tf.constant([float(v) for v in valores], dtype=spec.dtype)
        if spec.shape.rank == 2:
            t = tf.reshape(t, [-1, 1])
        return t

    def _exemplo(self, linha: dict) -> bytes:
        tf = self.tf
        f = {}
        for nome, tipo in FEATURES:
            v = linha[nome]
            if tipo == "STRING":
                f[nome] = tf.train.Feature(bytes_list=tf.train.BytesList(value=[str(v).encode()]))
            elif tipo == "INT64":
                f[nome] = tf.train.Feature(int64_list=tf.train.Int64List(value=[int(round(float(v)))]))
            else:
                f[nome] = tf.train.Feature(float_list=tf.train.FloatList(value=[float(v)]))
        return tf.train.Example(features=tf.train.Features(feature=f)).SerializeToString()

    def prever(self, linhas: list[dict]) -> list[float]:
        if self.modo == "features":
            feed = {n: self._tensor([l[n] for l in linhas], s) for n, s in self.entradas.items()}
        else:
            nome, spec = next(iter(self.entradas.items()))
            feed = {nome: self._tensor([self._exemplo(l) for l in linhas], spec)}
        try:
            saida = self.fn(**feed)[self.saida].numpy()
        except Exception as e:  # noqa: BLE001
            raise ErroBackend(f"falha na inferência do SavedModel: {e}") from e
        return [float(v) for v in saida.reshape(len(linhas), -1)[:, 0]]


# ---------------------------------------------------------------------------
# Backend 2: Endpoint do Vertex AI
# ---------------------------------------------------------------------------
class PreditorVertex:
    backend = "vertex"
    LOTE = 200   # o Vertex AI aceita até 1,5 MB por requisição; 200 escolas ficam bem abaixo

    def __init__(self, projeto: str, regiao: str, endpoint_id: str, url: Optional[str] = None):
        self.url = url or (f"https://{regiao}-aiplatform.googleapis.com/v1/projects/{projeto}"
                           f"/locations/{regiao}/endpoints/{endpoint_id}:predict")
        if self.url.startswith("http://"):          # servidor local, usado só nos testes
            import requests
            self.sessao = requests.Session()
        else:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
            cred, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            self.sessao = AuthorizedSession(cred)
        self.endpoint_id = endpoint_id
        log.info("backend Vertex AI configurado", extra={"campos": {"url": self.url}})

    @staticmethod
    def _json(valor: Any, tipo: str):
        if tipo == "STRING":
            return str(valor)
        if tipo == "INT64":
            return int(round(float(valor)))
        return float(valor)

    def assinatura(self) -> dict:
        return {"modo": "vertex_endpoint", "url": self.url}

    def prever(self, linhas: list[dict]) -> list[float]:
        saida: list[float] = []
        for i in range(0, len(linhas), self.LOTE):
            inst = [{n: self._json(l[n], TIPOS[n]) for n in NOMES} for l in linhas[i:i + self.LOTE]]
            try:
                r = self.sessao.post(self.url, json={"instances": inst}, timeout=30)
            except Exception as e:  # noqa: BLE001
                raise ErroBackend(f"falha ao chamar o endpoint do Vertex AI: {e}") from e
            if r.status_code != 200:
                raise ErroBackend(f"o Vertex AI respondeu {r.status_code}: {r.text[:300]}")
            pred = r.json().get("predictions") or []
            if len(pred) != len(inst):
                raise ErroBackend(f"o Vertex AI devolveu {len(pred)} predições para {len(inst)} instâncias")
            saida.extend(_primeiro_numero(p) for p in pred)
        return saida


# ---------------------------------------------------------------------------
def criar_preditor(cfg) -> tuple[Any, Optional[Metadados]]:
    base = baixar_diretorio(cfg.modelo_uri, Path(cfg.dir_local)) if cfg.modelo_uri else None
    meta = Metadados.ler(base / "metadados_modelo.json") if base else None
    if cfg.backend == "vertex":
        if not (cfg.vertex_endpoint_id or cfg.vertex_url):
            raise ErroBackend("BACKEND=vertex exige VERTEX_ENDPOINT_ID")
        return PreditorVertex(cfg.projeto, cfg.regiao, cfg.vertex_endpoint_id, cfg.vertex_url), meta
    if base is None:
        raise ErroBackend("BACKEND=savedmodel exige MODELO_URI (gs://... ou pasta local)")
    dir_modelo = base / "modelo" if (base / "modelo" / "saved_model.pb").exists() else base
    if not (dir_modelo / "saved_model.pb").exists():
        raise ErroBackend(f"saved_model.pb não encontrado em {base} nem em {base / 'modelo'}")
    return PreditorSavedModel(dir_modelo), meta
