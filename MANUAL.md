# Manual da equipe: entregar e validar o Trabalho 2 de ponta a ponta

Este manual leva a equipe do repositório até a apresentação. Siga as partes na ordem. Cada passo diz o comando, quanto tempo leva e o que conferir antes de seguir. As partes A a E se fazem uma vez, com calma, dias antes. A parte F é o ensaio geral. A parte H é o dia da apresentação.

**Projeto:** `platinum-avenue-505512-t5` · **Dataset:** `trabalho_pdm` · **Região:** `us-central1`

| Parte | O que acontece | Tempo |
|---|---|---|
| A | preparar o Cloud Shell | 10 min |
| B | testes locais (opcional, recomendado) | 5 a 10 min |
| C | implantar o pipeline de streaming | 10 min |
| D | exportar e registrar o modelo no Vertex AI | 5 min |
| E | implantar e validar a API REST | 15 min |
| F | ensaio geral da demonstração | 40 min |
| G | endpoint do Vertex AI (opcional, tem custo por hora) | 25 min |
| H | o dia da apresentação | |
| I | depois da apresentação | 5 min |
| J | problemas comuns | |
| K | checklist de entrega | |

---

## Como falar dos dados (combinem isto antes)

Os registros que fluem pelo Pub/Sub **não são dados coletados em 2023**. A frase que a equipe deve usar, nos slides e na fala, é esta:

> "Os registros do fluxo partem de 3.106 escolas reais de 2019 do INEP. Sobre elas aplicamos ruído controlado e defeitos de propósito, reconstruímos o corpo docente a partir das proporções reais de cada escola e gravamos o ano 2023 para simular a chegada de um ano novo. O enunciado permite um gerador de eventos simulando registros em tempo real."

Se perguntarem por que não usar dados de 2021 ou 2023: a gold da fase 1 só tem 2013, 2015 e 2019, porque o JOIN interno das cinco fontes eliminou os outros anos. 2019 é o único ano completo que o modelo não viu no treino.

---

## Parte A. Preparar o Cloud Shell (uma vez)

**A1. Permissões.** Quem roda os scripts precisa do papel **Proprietário** (Owner) do projeto, porque eles criam contas de serviço e dão permissões. Se não for o caso, peça ao dono do projeto os papéis Service Account Admin, Project IAM Admin, Cloud Run Admin, Cloud Build Editor, Pub/Sub Admin, BigQuery Admin, Storage Admin e Vertex AI Administrator.

**A2. Clonar o repositório.** Abra o [Cloud Shell](https://shell.cloud.google.com) e rode:

```bash
git clone https://github.com/Giuliohbb/pdm-ideb-lote-streaming.git && cd pdm-ideb-lote-streaming
gcloud config set project platinum-avenue-505512-t5
```

**A3. Ambiente Python.** Um ambiente virtual evita conflito com o que já vem no Cloud Shell:

```bash
python3 -m venv ~/venv-pdm && source ~/venv-pdm/bin/activate
pip install -r requirements-local.txt -r produtor/requirements.txt -r api/requirements-cliente.txt
```

Gere os dados do fluxo (a pasta `dados/` não fica no Git; cerca de 20 segundos, sempre os mesmos arquivos):

```bash
python gerador/gerar_bronzes_fluxo.py --semente semente --saida dados --n-escolas 2000 --ano 2023
```

Toda vez que abrir um Cloud Shell novo, rode `source ~/venv-pdm/bin/activate` e `cd ~/pdm-ideb-lote-streaming`.

**A4. Conferir o que a fase 1 deixou.** O modelo e a gold precisam existir:

```bash
bq ls trabalho_pdm | grep -E "gold_ideb_features|modelo_ideb"
```

Confira: aparecem `gold_ideb_features` (TABLE) e `modelo_ideb` (MODEL).

---

## Parte B. Testes locais, sem gastar nada na nuvem (opcional, recomendado)

**B1. Pipeline de streaming** (cerca de 1 minuto):

```bash
python testes/teste_ponta_a_ponta.py --dados dados
```

Confira: a última linha é `RESULTADO: TUDO CERTO`, com 69.078 mensagens no destino esperado e a gold de 1.825 escolas idêntica ao gabarito.

**B2. API de predição.** Este teste instala o TensorFlow (cerca de 1,5 GB). No Cloud Shell cabe, mas é mais confortável no computador de alguém da equipe:

```bash
pip install -r api/requirements.txt httpx
python testes/teste_api.py
```

Confira: `RESULTADO: TUDO CERTO`, com as 36 verificações em `[OK]`.

---

## Parte C. Pipeline de streaming

**C1. Implantar** (de 5 a 8 minutos):

```bash
bash infra/implantar.sh
```

O script habilita as APIs, cria as tabelas e views de streaming, as contas de serviço, os tópicos (principal e dead letter), constrói a imagem, publica o serviço `limpeza-ideb`, cria as assinaturas e o job `gold-ideb`. Pode ser rodado de novo sem duplicar nada.

Confira:

```bash
# deve aparecer limpeza-ideb
gcloud run services list --region us-central1
# deve aparecer gold-ideb
gcloud run jobs list --region us-central1
# devem aparecer ideb-bronze-eventos e ideb-bronze-dlq
gcloud pubsub topics list --format="value(name)"
# devem aparecer silver_stream_*, gold_ideb_features_stream, predicoes_ideb_stream e as demais
bq ls trabalho_pdm | grep stream
```

**C2. Teste de fumaça com 20 escolas** (3 minutos):

```bash
gcloud run jobs execute gold-ideb --region us-central1
python produtor/produtor.py --projeto platinum-avenue-505512-t5 --topico ideb-bronze-eventos \
  --dados dados --escolas 20 --taxa 50
```

Espere 1 minuto e rode, no console do BigQuery, as consultas 1, 2 e 6 de `sql/03_monitoramento_demo.sql`. Confira: há mensagens aceitas, descartadas e em quarentena, e escolas na `gold_ideb_features_stream`.

**C3. (Opcional) Bronzes do fluxo em repouso** no Cloud Storage e no BigQuery (`bronze_fluxo_*`):

```bash
bash infra/salvar_bronzes_fluxo.sh
```

---

## Parte D. Exportar e registrar o modelo no Vertex AI

> **Já feito na implantação real.** O modelo em produção é a **v2**, a cópia corrigida descrita em `docs/NOTAS_DA_IMPLANTACAO.md`. Não rode esta parte de novo sobre a v2: o script exporta do BigQuery e regeneraria o artefato sem a correção. Os passos abaixo descrevem como a v1 foi registrada.

**D1. Registrar** (de 3 a 5 minutos):

```bash
bash infra/registrar_modelo.sh
```

O que acontece:
1. descobre a localização do dataset e cria o bucket `platinum-avenue-505512-t5-modelos-ideb` numa localização compatível;
2. `EXPORT MODEL` grava o `modelo_ideb` como TensorFlow SavedModel em `gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v1/modelo/`;
3. monta o `metadados_modelo.json` (médias de treino, categorias, métricas oficiais de 2019) e o grava ao lado;
4. registra o artefato no Vertex AI Model Registry como `modelo-ideb`, versão `v1`;
5. grava `infra/modelo_registrado.env`, que os próximos scripts leem.

Confira na saída:
- a listagem do bucket mostra `saved_model.pb` e a pasta `variables/`;
- a linha `metadados gravados ... 85 features (6 categóricas)`;
- a tabela de versões mostra a versão com o alias `v1`.

**D2. Conferir no console** e tirar um print para a entrega: **Vertex AI > Model Registry > modelo-ideb**. Devem aparecer a versão `v1`, os rótulos (`origem: bigquery-ml`, `versao: v1`) e a descrição com R² e MAE. Pelo terminal:

```bash
gcloud ai models list --region us-central1
gcloud storage cat gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v1/metadados_modelo.json | head -30
```

**D3. Publicar uma versão nova no futuro** (se o modelo for retreinado): `VERSAO=v2 bash infra/registrar_modelo.sh`. A versão anterior continua no registro.

---

## Parte E. API REST no Cloud Run

**E1. Implantar** (de 8 a 12 minutos, porque o Cloud Build instala o TensorFlow na imagem):

```bash
bash infra/implantar_api.sh
```

No fim, o script testa `/saude` e faz uma predição com `api/exemplos/uma_escola.json`. Guarde a URL:

```bash
source infra/modelo_registrado.env && echo $URL_API
```

**E2. Conferir as rotas.**

```bash
curl -s $URL_API/saude | python3 -m json.tool
curl -s $URL_API/modelo | python3 -m json.tool | head -40
curl -s -X POST $URL_API/predicao -H "Content-Type: application/json" -d @api/exemplos/tres_escolas.json | python3 -m json.tool
curl -s -X POST $URL_API/predicao -H "Content-Type: application/json" -d @api/exemplos/com_avisos.json | python3 -m json.tool
curl -s -X POST $URL_API/predicao -H "Content-Type: application/json" -d @api/exemplos/entrada_invalida.json | python3 -m json.tool
```

Confira:
- `/saude` responde `"status": "ok"` com `"versao": "v2"` e `"backend": "savedmodel"`;
- `/modelo` mostra a assinatura com 85 entradas e as métricas oficiais;
- `tres_escolas` devolve três previsões entre 3 e 6;
- `com_avisos` devolve a previsão e um aviso de que o INSE nulo foi preenchido com a média de treino;
- `entrada_invalida` devolve **422** apontando os três problemas (`ano` com texto, `inse` ausente e um campo desconhecido).

Abra também `$URL_API/docs` no navegador: é a documentação interativa, com um exemplo pronto em **POST /predicao > Try it out > Execute**.

**E3. A prova de que a API serve o mesmo modelo do BigQuery** (1 minuto):

```bash
python api/cliente_validacao.py --url $URL_API --fonte lote --n 300 | tee validacao_lote.txt
```

O script pega 300 escolas reais de 2019 da gold do lote, calcula o `ML.PREDICT` no BigQuery, envia as mesmas escolas à API e compara. Confira: `dentro da tolerância de 0.001: 300 de 300` e `RESULTADO: OK`. Guarde o arquivo como evidência.

Se der divergência, veja a parte J antes de qualquer outra coisa: o script mostra as três piores escolas.

---

## Parte F. Ensaio geral (faça pelo menos uma vez, com cronômetro)

**F1. Começar do zero** (3 minutos):

```bash
bash infra/limpar_ambiente.sh zerar
```

Aguarde 2 minutos antes de publicar (a assinatura BigQuery precisa reconhecer as tabelas recriadas).

**F2. Ligar o consolidador e pré-carregar** (5 minutos):

```bash
gcloud run jobs execute gold-ideb --region us-central1
python produtor/produtor.py --projeto platinum-avenue-505512-t5 --topico ideb-bronze-eventos \
  --dados dados --escolas 1700 --taxa 0
```

**F3. O trecho "ao vivo"** (4 minutos de envio):

```bash
python produtor/produtor.py --projeto platinum-avenue-505512-t5 --topico ideb-bronze-eventos \
  --dados dados --pular-escolas 1700 --escolas 300 --taxa 40
```

Enquanto envia, rode as consultas de `sql/03_monitoramento_demo.sql` na ordem do roteiro da parte H.

**F4. A API com dados que acabaram de chegar.** No BigQuery, pegue uma escola recém-consolidada:

```sql
SELECT id_escola, versao, atualizado_em
FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`
ORDER BY atualizado_em DESC LIMIT 5;
```

E peça a previsão dela à API:

```bash
curl -s $URL_API/escolas/<ID_ESCOLA>/predicao | python3 -m json.tool
```

Confira: `origem_dado: streaming`, e `ideb_previsto_api` igual a `ideb_previsto_bigquery` (a `diferenca_api_bigquery` fica abaixo de 0,001). Se a predição do BigQuery ainda vier `null`, espere o próximo ciclo do job (30 segundos).

Também vale testar uma escola real de 2019, que está na gold do lote. Pegue qualquer `id_escola` de `gold_ideb_features` com `ano = 2019`:

```bash
curl -s "$URL_API/escolas/<ID_REAL>/predicao?ano=2019" | python3 -m json.tool
```

**F5. Validação com a gold de streaming:**

```bash
python api/cliente_validacao.py --url $URL_API --fonte streaming --n 300 | tee validacao_streaming.txt
```

Confira: `RESULTADO: OK`. A saída também traz a latência (mediana e p95) e o MAE da API contra o IDEB das escolas do fluxo.

**F6. Fechamento analítico:** rode `sql/04_avaliacao_modelo.sql` (métricas na gold de streaming contra a avaliação de 2019, erro por região e rede, efeito do campo `ano`).

**F7. Anote o tempo de cada bloco** e ajuste o roteiro da parte H.

---

## Parte G. Endpoint do Vertex AI (opcional)

A API já cumpre o enunciado carregando o modelo. O endpoint mostra a outra forma citada ("consultar o endpoint do Vertex AI"). **Ele cobra US$ 0,1095 por hora de nó (n1-standard-2 em us-central1) enquanto estiver implantado, mesmo sem uso.** Implante no dia e remova logo depois.

**G1. Implantar** (de 10 a 20 minutos):

```bash
bash infra/endpoint_vertex.sh implantar
```

Confira: o script termina com um teste direto no endpoint (`gcloud ai endpoints predict`) que devolve uma previsão.

**G2. Fazer a API consultar o endpoint** (5 minutos, reaproveita a imagem):

```bash
source infra/modelo_registrado.env
BACKEND=vertex VERTEX_ENDPOINT_ID=$VERTEX_ENDPOINT_ID bash infra/implantar_api.sh
# deve responder "backend": "vertex"
curl -s $URL_API/saude
python api/cliente_validacao.py --url $URL_API --fonte lote --n 100
```

**G3. Voltar ao padrão e remover o endpoint:**

```bash
BACKEND=savedmodel bash infra/implantar_api.sh
bash infra/endpoint_vertex.sh remover
```

---

## Parte H. O dia da apresentação

### Papéis sugeridos

| Papel | Responsabilidade |
|---|---|
| Narrador | conduz os slides e a fala |
| Operador do Cloud Shell | produtor, `curl`, `cliente_validacao.py` |
| Operador do BigQuery | consultas de `sql/03` e `sql/04`, já abertas em abas |
| Operador dos consoles e plano B | Model Registry, Cloud Run, Cloud Logging; acompanha erros e aciona o plano B |

### Uma hora antes

1. `bash infra/limpar_ambiente.sh zerar` e aguarde 2 minutos.
2. Se for mostrar o endpoint: `bash infra/endpoint_vertex.sh implantar` (leva até 20 minutos).
3. Deixe a API sem partida a frio:
   ```bash
   gcloud run services update api-ideb --region us-central1 --min-instances 1
   ```
4. Abra as abas: Cloud Shell (na pasta, com o ambiente ativado e `source infra/modelo_registrado.env`), BigQuery com as consultas de `sql/03` e `sql/04`, **Vertex AI > Model Registry > modelo-ideb**, **Cloud Run > api-ideb**, **Cloud Logging** filtrado por `resource.labels.service_name="api-ideb"`, e `$URL_API/docs`.

### Quinze minutos antes

O job da gold roda por 55 minutos, então ele é iniciado agora para cobrir a apresentação inteira:

```bash
gcloud run jobs execute gold-ideb --region us-central1
python produtor/produtor.py --projeto platinum-avenue-505512-t5 --topico ideb-bronze-eventos \
  --dados dados --escolas 1700 --taxa 0
```

Confira, antes de começar, que a consulta 6 de `sql/03` já mostra escolas na gold e predições.

### Roteiro ao vivo (cerca de 18 minutos)

**Bloco 1. Streaming (6 min).**
1. Dispare o trecho ao vivo: `python produtor/produtor.py ... --pular-escolas 1700 --escolas 300 --taxa 40`.
2. Consulta 1: as mensagens chegando minuto a minuto.
3. Consultas 2 e 3: o placar da limpeza e as regras aplicadas.
4. Consulta 4 ou 5: a quarentena com o JSON corrompido, ou um registro antes e depois da limpeza.
5. Consulta 6: o funil bronze, silver, gold, predições.
6. Consulta 9: lote e streaming lado a lado na `vw_gold_ideb_unificada`.
7. Consulta 10: dados tardios atualizando a gold (versão 2).

**Bloco 2. O modelo em produção (3 min).**
1. Console do Vertex AI Model Registry: `modelo-ideb`, versão 2, descrição e o local do artefato.
2. `gcloud storage ls -r gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v2/` mostra o SavedModel e o `metadados_modelo.json`.

**Bloco 3. A API (6 min).**
1. `$URL_API/docs`: o contrato da API e o **Try it out** com o exemplo.
2. `curl` com `tres_escolas.json`, depois com `entrada_invalida.json` (422 explicando cada erro).
3. A escola que acabou de chegar pelo fluxo: a consulta da parte F4 e `curl $URL_API/escolas/<ID>/predicao`, mostrando a previsão da API igual à do BigQuery.
4. Cloud Logging: uma linha JSON por requisição, com a latência.
5. Se o endpoint estiver implantado: mostre-o no console e explique a diferença de custo.

**Bloco 4. Validação e fechamento (3 min).**
1. `python api/cliente_validacao.py --url $URL_API --fonte streaming --n 200`: concordância com o BigQuery ML e latência.
2. `sql/04`: métricas no streaming e o efeito do campo `ano` (0,26 ponto).

### Plano B

| Se acontecer | Faça |
|---|---|
| o produtor travar ou a rede cair | as 1.700 escolas pré-carregadas já sustentam todas as consultas; siga o roteiro |
| a consulta 1 não mostrar mensagens novas | confira o terminal do produtor; mostre a consulta 7 (latência) com os dados já carregados |
| a API demorar na primeira chamada | é a partida a frio; o `--min-instances 1` evita isso; espere 20 segundos |
| a API responder 503 | `gcloud run services logs read api-ideb --region us-central1 --limit 30` e veja a parte J |
| o tempo apertar | pule as consultas 4 e 10 e o bloco do endpoint |

---

## Parte I. Depois da apresentação

```bash
# se for manter a API no ar
gcloud run services update api-ideb --region us-central1 --min-instances 0
# se usou o endpoint
bash infra/endpoint_vertex.sh remover
# remove tudo que roda
bash infra/limpar_ambiente.sh desligar
```

O `desligar` remove assinaturas, tópicos, o serviço de limpeza, o job, a API e o endpoint. Ficam as tabelas do BigQuery, o artefato no Cloud Storage e o registro no Vertex AI, que não custam nada parados e servem de evidência. Se o professor for testar a API depois, não rode o `desligar`; deixe só `--min-instances 0`.

---

## Parte J. Problemas comuns

| Sintoma | Causa provável e solução |
|---|---|
| `PERMISSION_DENIED` ao criar conta de serviço ou dar papel | falta o papel de Proprietário; ver A1 |
| erro de localização no `EXPORT MODEL` | o bucket precisa ficar na localização do dataset; o script mostra as duas. Para dataset fora dos EUA, rode `REGIAO=<região do dataset> bash infra/registrar_modelo.sh` |
| o upload do Vertex AI reclama da região do artefato | o bucket e o Vertex AI precisam estar na mesma região; use outro nome de bucket: `BUCKET=<novo-nome> bash infra/registrar_modelo.sh` |
| `alias already exists` no upload | a versão já foi registrada; para uma nova, use `VERSAO=v2` |
| `/saude` responde 503 | leia os logs (`gcloud run services logs read api-ideb --region us-central1 --limit 30`). Mensagens comuns: sem permissão no bucket (rode de novo o `implantar_api.sh`), `MODELO_URI` errado, ou "o modelo não espera as 85 features" (lista as que faltam) |
| erro sobre `allUsers` ao implantar a API | uma política da organização impede API pública. Rode `PUBLICA=nao bash infra/implantar_api.sh` e chame com `-H "Authorization: Bearer $(gcloud auth print-identity-token)"`; no `cliente_validacao.py`, acrescente `--privada` |
| `cliente_validacao.py` acusa divergência | veja as três piores escolas. Se só divergirem escolas com avisos (nulos ou categoria nova), o tratamento de nulos do artefato difere do da API; anote e mostre como limitação. Se todas divergirem, confirme que a API está na versão do modelo usada pelo BigQuery (`/modelo`) |
| o push do Pub/Sub dá 403 logo após implantar | as permissões levam 1 ou 2 minutos para valer; o Pub/Sub tenta de novo sozinho |
| a assinatura BigQuery para de gravar depois do `zerar` | aguarde 2 minutos; se persistir, rode `bash infra/implantar.sh` de novo |
| o endpoint demora | 10 a 20 minutos é normal; acompanhe com `bash infra/endpoint_vertex.sh status` |
| a imagem da API demora para construir | normal: o TensorFlow tem cerca de 300 MB; leva de 5 a 8 minutos no Cloud Build |

---

## Parte K. Checklist de entrega

- [ ] link do repositório no GitHub (README, manual, códigos, testes e scripts; os dados se geram com o gerador)
- [ ] apresentação final (link do deck; ele exporta para PowerPoint e PDF)
- [ ] print do **Vertex AI Model Registry** com `modelo-ideb` versão 2
- [ ] print do **Cloud Run** com `limpeza-ideb` e `api-ideb`, e do job `gold-ideb`
- [ ] print de `$URL_API/docs` e de uma resposta de `POST /predicao`
- [ ] `validacao_lote.txt` e `validacao_streaming.txt` (saídas do `cliente_validacao.py`)
- [ ] print da `vw_gold_ideb_unificada` com as duas origens (consulta 9 de `sql/03`)
- [ ] URL da API anotada (`echo $URL_API`)
- [ ] endpoint do Vertex AI removido, se foi usado
