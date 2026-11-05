"""Teste local da API de predição, sem GCP.

Usa um modelo de teste no formato do EXPORT MODEL do BigQuery ML, montado com os pesos reais do
modelo_ideb (testes/modelo_de_teste.py), e uma referência em numpy que faz a mesma conta do
ML.PREDICT. Verificações:

  1. a lista de features da API é a mesma da gold (pipeline/esquema.py);
  2. o metadados_modelo.json gerado pelo script de produção tem 85 features, 6 categóricas e as médias de treino;
  3. a API carrega as três variantes de assinatura e prevê as 1.825 escolas da gold de streaming
     esperada e as 3.106 escolas reais de 2019 com o mesmo resultado da referência;
  4. a validação de entrada rejeita campo faltando, tipo errado, campo desconhecido e lotes fora do limite;
  5. nulos e categorias seguem as regras do ML.PREDICT (média de treino, categoria nova com peso zero)
     e a normalização das categorias reproduz a do serviço de limpeza;
  6. o backend Vertex AI (servidor local que imita o endpoint) devolve as mesmas previsões e erros viram 502;
  7. a rota GET /escolas/{id}/predicao monta a resposta a partir da gold (consulta simulada);
  8. sem modelo, /saude responde 503 e /predicao também;
  9. o cliente_validacao.py, rodando contra a API de verdade (uvicorn), aprova a concordância;
 10. o artefato é baixado de gs://... (cliente do Cloud Storage simulado) e carregado.

Uso: python testes/teste_api.py
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "api"))
sys.path.insert(0, str(RAIZ / "pipeline"))
sys.path.insert(0, str(RAIZ / "testes"))

import esquema as E  # noqa: E402
import modelo_de_teste as MT  # noqa: E402
from app.features import FEATURES, NOMES  # noqa: E402

TUDO_OK = True


def ok(cond: bool, msg: str) -> bool:
    global TUDO_OK
    print(("  [OK]   " if cond else "  [FALHA] ") + msg)
    TUDO_OK &= bool(cond)
    return cond


def ler_gold(caminho: Path) -> list[dict]:
    tipos = dict(E.GOLD)
    with open(caminho, encoding="utf-8") as f:
        linhas = []
        for r in csv.DictReader(f):
            d = {}
            for c in E.GOLD_COLUNAS:
                v = r.get(c, "")
                if v == "":
                    d[c] = None
                elif tipos[c] == "INT64":
                    d[c] = int(float(v))
                elif tipos[c] == "FLOAT64":
                    d[c] = float(v)
                else:
                    d[c] = v
            linhas.append(d)
    return linhas


def instancia(linha: dict) -> dict:
    d = {n: linha[n] for n in NOMES}
    d["id_escola"], d["ideb"] = linha["id_escola"], linha["ideb"]
    return d


def cliente(env: dict):
    """TestClient com a API recarregada para as variáveis de ambiente dadas."""
    from fastapi.testclient import TestClient
    for k in ("BACKEND", "MODELO_URI", "VERTEX_URL", "VERTEX_ENDPOINT_ID", "DIR_LOCAL_MODELO"):
        os.environ.pop(k, None)
    os.environ.update(env)
    import logging
    from app import main
    logging.getLogger("api_ideb").setLevel(logging.WARNING)   # silencia os logs de cada requisição no teste
    return TestClient(main.app)


def prever_todas(c, linhas: list[dict], lote: int = 500) -> list[float]:
    saida = []
    for i in range(0, len(linhas), lote):
        r = c.post("/predicao", json={"instancias": [instancia(l) for l in linhas[i:i + lote]]})
        assert r.status_code == 200, r.text[:500]
        saida += [p["ideb_previsto"] for p in r.json()["predicoes"]]
    return saida


class ServidorVertexFalso(BaseHTTPRequestHandler):
    """Imita o :predict de um Endpoint do Vertex AI com o contêiner do TF Serving."""
    ref = None
    falhar = False
    tipos_ok = True

    def do_POST(self):  # noqa: N802
        corpo = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if ServidorVertexFalso.falhar:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"error": "modelo nao implantado"}')
            return
        for inst in corpo["instances"]:
            for n, t in FEATURES:
                esperado = {"INT64": int, "FLOAT64": float, "STRING": str}[t]
                if not isinstance(inst[n], esperado):
                    ServidorVertexFalso.tipos_ok = False
        pred = [{"predicted_ideb": [ServidorVertexFalso.ref.prever(i)]} for i in corpo["instances"]]
        dados = json.dumps({"predictions": pred, "deployedModelId": "123"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(dados)

    def log_message(self, *a):  # silencia o log do servidor de teste
        pass


def porta_livre() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main() -> int:
    ref = MT.Referencia()
    gold_stream = ler_gold(RAIZ / "dados" / "gabarito" / "gold_esperada.csv")
    gold_2019 = ler_gold(RAIZ / "semente" / "semente_gold_2019.csv")
    tmp = Path(tempfile.mkdtemp(prefix="teste_api_"))

    print("1) Features da API = gold sem id_escola e ideb")
    ok(FEATURES == [(c, t) for c, t in E.GOLD if c not in ("id_escola", "ideb")], "85 features, mesmos nomes, tipos e ordem")

    print("2) metadados_modelo.json (modelo/montar_metadados.py)")
    MT.gravar_metadados(tmp)
    meta = json.loads((tmp / "metadados_modelo.json").read_text(encoding="utf-8"))
    cats = [f for f in meta["features"] if f["tipo"] == "categorica"]
    ok(meta["quantidade_features"] == 85 and len(cats) == 6, f"{meta['quantidade_features']} features, {len(cats)} categóricas")
    ird = next(f for f in meta["features"] if f["nome"] == "ird_media_regularidade_docente")
    ok(abs(ird["media_treino"] - ref.media["ird_media_regularidade_docente"]) < 1e-12, "média de treino preservada")
    ok(len(next(f for f in cats if f["nome"] == "id_municipio")["categorias_no_treino"]) == 5383, "5.383 municípios vistos no treino")

    print("3) Três variantes de assinatura do SavedModel x referência (ML.PREDICT em numpy)")
    esperado_stream = [ref.prever(l) for l in gold_stream]
    esperado_2019 = [ref.prever(l) for l in gold_2019]
    for variante, tol in (("features", 1e-6), ("coluna32", 2e-3), ("tf_example", 2e-3)):
        destino = tmp / variante
        MT.construir(destino, variante)
        with cliente({"MODELO_URI": str(destino)}) as c:
            s = c.get("/saude")
            ok(s.status_code == 200 and s.json()["modelo"]["backend"] == "savedmodel", f"[{variante}] /saude 200")
            api = prever_todas(c, gold_stream)
            dif = max(abs(a - b) for a, b in zip(api, esperado_stream))
            ok(len(api) == len(gold_stream) and dif <= tol,
               f"[{variante}] {len(api)} escolas da gold de streaming, diferença máxima {dif:.1e} (tolerância {tol:g})")
            if variante == "features":
                api19 = prever_todas(c, gold_2019)
                dif19 = max(abs(a - b) for a, b in zip(api19, esperado_2019))
                mae = sum(abs(a - l["ideb"]) for a, l in zip(api19, gold_2019)) / len(gold_2019)
                ok(dif19 <= tol, f"[{variante}] {len(api19)} escolas reais de 2019, diferença máxima {dif19:.1e}; MAE contra o IDEB real {mae:.3f}")
                m = c.get("/modelo").json()
                ok(m["assinatura"]["modo"] == "features" and len(m["assinatura"]["entradas"]) == 85
                   and m["metadados"]["quantidade_features"] == 85, "/modelo mostra assinatura e metadados")

    print("4) Validação de entrada")
    with cliente({"MODELO_URI": str(tmp / "features")}) as c:
        base = instancia(gold_stream[0])
        falta = {k: v for k, v in base.items() if k != "inse"}
        r = c.post("/predicao", json={"instancias": [falta]})
        ok(r.status_code == 422 and any("inse" in d["campo"] for d in r.json()["detalhes"]), "campo faltando: 422 apontando 'inse'")
        r = c.post("/predicao", json={"instancias": [dict(base, ano="dois mil")]})
        ok(r.status_code == 422, "tipo errado em 'ano': 422")
        r = c.post("/predicao", json={"instancias": [dict(base, campo_inventado=1)]})
        ok(r.status_code == 422, "campo desconhecido: 422")
        ok(c.post("/predicao", json={"instancias": []}).status_code == 422, "lista vazia: 422")
        ok(c.post("/predicao", json={"instancias": [base] * 1001}).status_code == 422, "1.001 escolas: 422 (limite 1.000)")
        r = c.post("/predicao", json={"instancias": [base]})
        p = r.json()["predicoes"][0]
        ok(r.status_code == 200 and p["id_escola"] == base["id_escola"] and p["erro_absoluto"] is not None,
           f"escola {base['id_escola']}: previsto {p['ideb_previsto']:.3f}, informado {p['ideb_informado']}")

        print("5) Nulos, categorias novas e normalização")
        nulo = dict(base, inse=None)
        p = c.post("/predicao", json={"instancias": [nulo]}).json()["predicoes"][0]
        ok(abs(p["ideb_previsto"] - ref.prever(dict(gold_stream[0], inse=None))) < 1e-6
           and any("média de treino" in a for a in p["avisos"]), "inse nulo: preenchido com a média de treino e avisado")
        nova = dict(base, id_municipio="9999999")
        p = c.post("/predicao", json={"instancias": [nova]}).json()["predicoes"][0]
        ok(abs(p["ideb_previsto"] - ref.prever(dict(gold_stream[0], id_municipio="9999999"))) < 1e-6
           and any("não apareceu no treino" in a for a in p["avisos"]), "município novo: peso zero e aviso")
        p = c.post("/predicao", json={"instancias": [dict(base, tipo_localizacao_diferenciada=None)]}).json()["predicoes"][0]
        ok(any("categoria ausente" in a for a in p["avisos"]), "categoria nula: tratada como ausente e avisada")
        rede_txt = {"1": "Federal", "2": "Estadual", "3": "municipal", "4": "privada"}[base["rede"]]
        variado = dict(base, rede=rede_txt, sigla_uf=base["sigla_uf"].lower(), id_municipio=int(base["id_municipio"]),
                       tipo_localizacao_diferenciada=int(float(base["tipo_localizacao_diferenciada"] or 0)),
                       icg_nivel_complexidade_gestao_escola=(base["icg_nivel_complexidade_gestao_escola"] or "nivel 1").replace("nivel", "Nível"))
        a = c.post("/predicao", json={"instancias": [base, variado]}).json()["predicoes"]
        ok(abs(a[0]["ideb_previsto"] - a[1]["ideb_previsto"]) < 1e-9 and not a[1]["avisos"],
           "rede por extenso, UF minúscula, município numérico, 'Nível N' e código sem '.0' dão a mesma previsão")

    print("6) Backend Vertex AI (servidor local que imita o endpoint)")
    ServidorVertexFalso.ref = ref
    porta = porta_livre()
    srv = ThreadingHTTPServer(("127.0.0.1", porta), ServidorVertexFalso)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    with cliente({"BACKEND": "vertex", "MODELO_URI": str(tmp / "features"),
                  "VERTEX_URL": f"http://127.0.0.1:{porta}/v1/endpoint:predict"}) as c:
        ok(c.get("/saude").json()["modelo"]["backend"] == "vertex", "/saude informa backend vertex")
        api = prever_todas(c, gold_stream)
        dif = max(abs(a - b) for a, b in zip(api, esperado_stream))
        ok(dif < 1e-6, f"{len(api)} escolas via endpoint em lotes de 200, diferença máxima {dif:.1e}")
        ok(ServidorVertexFalso.tipos_ok, "inteiros vão como int, decimais como float e textos como string")
        ServidorVertexFalso.falhar = True
        r = c.post("/predicao", json={"instancias": [instancia(gold_stream[0])]})
        ok(r.status_code == 502 and "500" in r.json()["detail"], "endpoint com erro: a API responde 502 com a causa")
        ServidorVertexFalso.falhar = False
    srv.shutdown()

    print("7) GET /escolas/{id}/predicao (consulta ao BigQuery simulada)")
    from app import fonte_bigquery
    original = fonte_bigquery.buscar_escola
    lote_row = dict(gold_2019[0], origem_dado="lote", ideb_previsto_bigquery=None)
    stream_row = dict(gold_stream[0], origem_dado="streaming", ideb_previsto_bigquery=ref.prever(gold_stream[0]))
    fonte_bigquery.buscar_escola = lambda p, d, i, a: ([lote_row] if i == lote_row["id_escola"] else
                                                       [stream_row] if i == stream_row["id_escola"] else [])
    try:
        with cliente({"MODELO_URI": str(tmp / "features")}) as c:
            r = c.get(f"/escolas/{stream_row['id_escola']}/predicao").json()
            res = r["resultados"][0]
            ok(res["origem_dado"] == "streaming" and res["diferenca_api_bigquery"] < 1e-6,
               f"escola do fluxo {stream_row['id_escola']}: API {res['ideb_previsto_api']:.4f} = BigQuery {res['ideb_previsto_bigquery']:.4f}")
            r = c.get(f"/escolas/{lote_row['id_escola']}/predicao?ano=2019").json()["resultados"][0]
            ok(r["origem_dado"] == "lote" and r["ideb_previsto_bigquery"] is None,
               f"escola real de 2019 {lote_row['id_escola']}: IDEB {r['ideb_real']} previsto {r['ideb_previsto_api']:.2f}")
            ok(c.get("/escolas/12345678/predicao").status_code == 404, "escola inexistente: 404")
            ok(c.get("/escolas/abc/predicao").status_code == 422, "código fora do padrão de 8 dígitos: 422")
    finally:
        fonte_bigquery.buscar_escola = original

    print("8) Sem modelo")
    with cliente({"MODELO_URI": str(tmp / "nao_existe")}) as c:
        ok(c.get("/saude").status_code == 503, "/saude 503 quando o artefato não existe")
        ok(c.post("/predicao", json={"instancias": [instancia(gold_stream[0])]}).status_code == 503, "/predicao 503")

    print("9) cliente_validacao.py contra a API servida pelo uvicorn (HTTP de verdade)")
    arq = tmp / "gold_com_esperado.csv"
    with open(arq, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=E.GOLD_COLUNAS + ["esperado"])
        w.writeheader()
        for l, e in zip(gold_stream, esperado_stream):
            w.writerow({**{k: ("" if v is None else v) for k, v in l.items()}, "esperado": repr(e)})
    porta = porta_livre()
    env = dict(os.environ, MODELO_URI=str(tmp / "features"), BACKEND="savedmodel")
    env.pop("VERTEX_URL", None)
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(porta)],
                            cwd=RAIZ / "api", env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for _ in range(120):
            try:
                import requests
                if requests.get(f"http://127.0.0.1:{porta}/saude", timeout=1).status_code == 200:
                    break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)
        r = subprocess.run([sys.executable, str(RAIZ / "api" / "cliente_validacao.py"), "--url", f"http://127.0.0.1:{porta}",
                            "--fonte", "csv", "--csv", str(arq), "--n", "1825", "--latencia", "50"],
                           capture_output=True, text=True, timeout=600)
        print("     " + "\n     ".join(l for l in r.stdout.splitlines() if l.strip() and not l.startswith("GET /saude")))
        ok(r.returncode == 0, "cliente_validacao aprovou 1.825 escolas")
    finally:
        proc.terminate()
        try:
            saida_srv = proc.communicate(timeout=10)[0]
        except subprocess.TimeoutExpired:
            proc.kill()
            saida_srv = ""
    logs = [json.loads(l) for l in saida_srv.splitlines() if l.startswith("{")]
    ok(any(l.get("message") == "predicao" and "latencia_ms" in l for l in logs), "logs estruturados em JSON (um por requisição)")

    print("10) Download do artefato a partir de gs:// (cliente do Cloud Storage simulado)")
    import google.cloud.storage as gcs

    class BlobFalso:
        def __init__(self, nome, arq):
            self.name, self.arq = nome, arq

        def download_to_filename(self, destino):
            shutil.copyfile(self.arq, destino)

    class ClienteFalso:
        def __init__(self, *a, **k):
            pass

        def list_blobs(self, bucket, prefix):
            assert bucket == "bucket-teste"
            base = tmp / "features"
            for arq in sorted(base.rglob("*")):
                if arq.is_file():
                    nome = "modelo_ideb/v1/" + arq.relative_to(base).as_posix()
                    if nome.startswith(prefix):
                        yield BlobFalso(nome, arq)

    original_cliente = gcs.Client
    gcs.Client = ClienteFalso
    try:
        with cliente({"MODELO_URI": "gs://bucket-teste/modelo_ideb/v1", "DIR_LOCAL_MODELO": str(tmp / "baixado")}) as c:
            s = c.get("/saude")
            ok(s.status_code == 200 and (tmp / "baixado" / "modelo" / "saved_model.pb").exists()
               and (tmp / "baixado" / "metadados_modelo.json").exists(), "SavedModel e metadados baixados do bucket e carregados")
            r = c.post("/predicao", json={"instancias": [instancia(gold_stream[0])]}).json()["predicoes"][0]
            ok(abs(r["ideb_previsto"] - esperado_stream[0]) < 1e-6, "previsão igual à referência depois do download")
    finally:
        gcs.Client = original_cliente
        os.environ.pop("DIR_LOCAL_MODELO", None)

    shutil.rmtree(tmp, ignore_errors=True)
    print("\nRESULTADO:", "TUDO CERTO" if TUDO_OK else "HÁ FALHAS")
    return 0 if TUDO_OK else 1


if __name__ == "__main__":
    sys.exit(main())
