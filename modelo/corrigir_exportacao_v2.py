"""Corrige a ordem dos pesos do artefato v1 diagnosticado neste trabalho."""
import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import tensorflow as tf

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--origem", required=True, type=Path)
parser.add_argument("--destino", required=True, type=Path)
parser.add_argument("--uri-artefato", required=True)
args = parser.parse_args()

if args.destino.exists():
    raise SystemExit("O destino já existe. Nenhum arquivo foi alterado.")

meta = json.loads(
    (args.origem / "metadados_modelo.json").read_text()
)
if meta.get("versao") != "v1" or meta.get("correcao"):
    raise SystemExit("A origem deve ser a versão v1 original, sem correção.")

checkpoint = str(args.origem / "modelo/variables/variables")
leitor = tf.train.load_checkpoint(checkpoint)
nomes = sorted(nome for nome, _ in tf.train.list_variables(checkpoint))
valores = {nome: leitor.get_tensor(nome) for nome in nomes}
pesos = valores["weights"].copy()

esperados = np.array([
    0.569329014, 0.586363421,
    0.190945167, 0.513728779, 0.530031840,
    0.535845995, 0.574310100, 0.585244651, 1.110057608,
])

if pesos.shape != (5507, 1) or not np.allclose(
    pesos[5498:5507, 0], esperados, atol=1e-6, rtol=0
):
    raise SystemExit("Os pesos não correspondem ao artefato diagnosticado.")

# O grafo espera localização diferenciada antes de localização.
pesos[5498:5507] = np.concatenate([
    pesos[5500:5507].copy(),
    pesos[5498:5500].copy(),
])
valores["weights"] = pesos

shutil.copytree(args.origem, args.destino)
tf.raw_ops.SaveV2(
    prefix=str(args.destino / "modelo/variables/variables"),
    tensor_names=nomes,
    shape_and_slices=[""] * len(nomes),
    tensors=[tf.convert_to_tensor(valores[n]) for n in nomes],
)

meta["versao"] = "v2"
meta["uri_artefato"] = args.uri_artefato
meta["correcao"] = {
    "origem": "v1",
    "descricao": (
        "Reordenação dos blocos de pesos de tipo_localizacao "
        "e tipo_localizacao_diferenciada"
    ),
    "aplicada_em": datetime.now(timezone.utc).isoformat(),
    "validacao": "Pendente para esta execução; comparar com BigQuery ML.",
}
(args.destino / "metadados_modelo.json").write_text(
    json.dumps(meta, ensure_ascii=False, indent=2)
)
print(f"Cópia corrigida criada em {args.destino}. Valide antes de publicar.")
