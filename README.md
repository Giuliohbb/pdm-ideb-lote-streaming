# Trabalho 2: do lote ao streaming e à API de predição do IDEB

**Disciplina:** Processamento de Dados Massivos (UFG)
**Equipe:** Christian de Sousa Ramos, Igor Garbin Manzan Mazo, Giulio Henrique Borges Basso e Wallisson Policarpo Teodoro
**Projeto GCP:** `platinum-avenue-505512-t5` · **Dataset:** `trabalho_pdm`

**API em produção (Cloud Run):** https://api-ideb-1071424267162.us-central1.run.app · documentação interativa em [`/docs`](https://api-ideb-1071424267162.us-central1.run.app/docs)
**Modelo em produção:** `modelo-ideb` versão 2 no Vertex AI Model Registry (`9113764471460855808`), artefato em `gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v2/`

Este pacote evolui o pipeline em lote do Trabalho 1 para uma arquitetura híbrida e leva o modelo até produção. A gold histórica (2013, 2015 e 2019) continua onde está. Ao lado dela, registros das cinco fontes chegam continuamente pelo Pub/Sub, são limpos em tempo real, formam silvers de streaming e uma gold de streaming com exatamente as mesmas 87 colunas da `gold_ideb_features`. O `modelo_ideb` da fase 1 sai do BigQuery como artefato versionado, é registrado no Vertex AI Model Registry e passa a ser servido por uma API REST no Cloud Run.

O passo a passo para implantar, validar e apresentar está no **[MANUAL.md](MANUAL.md)**. Este README explica o que cada parte faz e por quê.

| Requisito do enunciado | Onde está |
|---|---|
| Pipeline em lote (base histórica e treino do modelo) | `lote/` (bronze, silver, gold e BigQuery ML do Trabalho 1) |
| Dados fluindo continuamente em JSON para o Pub/Sub | `produtor/produtor.py` e `dados/` (seção 3) |
| Pipeline em tempo real: consumir, limpar e gravar no BigQuery | `pipeline/` (serviço de limpeza e job da gold) |
| Relação evidente entre lote e streaming no BigQuery | `sql/01` e `sql/02`, view `vw_gold_ideb_unificada` (seção 7) |
| Modelo registrado no Vertex AI Model Registry ou como artefato de produção | `infra/registrar_modelo.sh` (seção 8): as duas coisas |
| API REST conteinerizada, serverless no Cloud Run, POST com JSON e resposta em tempo real | `api/` e `infra/implantar_api.sh` (seção 9) |

## 1. O que há no pacote

```
pdm-ideb-lote-streaming/
├── lote/                            pipeline em lote do Trabalho 1: bronze, silver, gold e modelo (SQL)
├── MANUAL.md                        passo a passo da equipe: implantar, validar, ensaiar e apresentar
├── dados/                           NÃO versionado: gerado pelo gerador (seção 3), as 5 bronzes do fluxo (2.000 escolas)
│   ├── bronze/                      as 5 bronzes sujas (.jsonl) + mensagens corrompidas
│   ├── fluxo/ordem_eventos.csv      ordem de envio usada pelo produtor
│   └── gabarito/                    gold esperada, destino esperado de cada mensagem, manifesto de defeitos
├── semente/                         os CSVs reais de 2019 exportados pela equipe (base do gerador)
├── gerador/gerar_bronzes_fluxo.py   gera as bronzes do fluxo a partir da semente real
├── produtor/produtor.py             código 1: bronzes -> Pub/Sub
├── pipeline/                        uma imagem Docker com os códigos 2 e 3
│   ├── servico_limpeza.py           código 2: Pub/Sub (push) -> limpeza -> silvers (Cloud Run Service)
│   ├── regras_limpeza.py            as regras de limpeza, uma função por tabela
│   ├── construir_gold.py            código 3: silvers -> gold em micro-lotes + ML.PREDICT (Cloud Run Job)
│   ├── sql_gold.py, gerar_sql.py    geração do SQL (DDL, views, MERGE) a partir do esquema
│   └── esquema.py                   colunas, domínios e faixas: fonte única de verdade
├── modelo/montar_metadados.py       monta o metadados_modelo.json que acompanha o artefato do modelo
├── api/                             a API REST de predição (FastAPI + TensorFlow), com Dockerfile
│   ├── app/                         main.py (rotas), preditor.py (modelo), esquema_api.py (contrato)
│   ├── exemplos/                    requisições prontas para curl
│   ├── cliente_validacao.py         compara a API com o ML.PREDICT do BigQuery e mede a latência
│   └── monitor_ao_vivo.py           painel de terminal para a demo: escolas chegando pelo streaming e previstas pela API, sem cliques
├── sql/                             01 tabelas · 02 views · 03 consultas da demo · 04 avaliação · 05/06 MERGEs
├── infra/
│   ├── implantar.sh                 pipeline de streaming (Pub/Sub, Cloud Run, job da gold)
│   ├── registrar_modelo.sh          EXPORT MODEL + metadados + Vertex AI Model Registry
│   ├── implantar_api.sh             imagem da API (Cloud Build) + Cloud Run
│   ├── endpoint_vertex.sh           opcional: endpoint de predição online do Vertex AI
│   ├── salvar_bronzes_fluxo.sh      opcional: bronzes do fluxo no Cloud Storage e no BigQuery
│   └── limpar_ambiente.sh           zerar dados de streaming ou desligar tudo
├── testes/
│   ├── teste_ponta_a_ponta.py       pipeline completo local, sem GCP, conferido com o gabarito
│   ├── teste_api.py                 API completa local, sem GCP, com modelo de teste no formato do EXPORT MODEL
│   └── modelo_de_teste.py           monta esse modelo de teste com os pesos reais do modelo_ideb
├── modelo/corrigir_exportacao_v2.py reproduz a correção do artefato v1 -> v2 (ver docs/NOTAS_DA_IMPLANTACAO.md)
├── evidencias/                      prints e saídas de validação da implantação real (historico/ guarda a divergência da v1)
└── docs/
    ├── CATALOGO_DEFEITOS.md         cada problema inserido, a regra que o limpa e o destino
    └── NOTAS_DA_IMPLANTACAO.md      o que foi corrigido na implantação real e como foi validado
```

## 2. Arquitetura

```
 STREAMING (fase 2)
 dados/bronze/*.jsonl ─► produtor.py ─► Pub/Sub ─┬─► assinatura BigQuery ─► bronze_stream_eventos (JSON cru)
                         (Cloud Shell)  tópico   │
                                                 └─► push (OIDC, 5 tentativas, dead letter)
                                                       ▼
                                   Cloud Run: limpeza-ideb (regras sem estado, mensagem a mensagem)
                                         │                                   │
                                         ▼                                   ▼
                            silver_stream_<tabela> (5)            auditoria_limpeza_stream
                                         │  a cada 30 s
                                         ▼
                     Cloud Run Job: gold-ideb (dedup, agregação de docentes, junção, marca d'água)
                                         ▼
                            gold_ideb_features_stream ──ML.PREDICT──► predicoes_ideb_stream
                                         │
 LOTE (fase 1)                           │  vw_gold_ideb_unificada = gold histórica UNION ALL gold de streaming
 gold_ideb_features ─────────────────────┘
        │
        ▼ treino (2013 e 2015)
 MODELO EM PRODUÇÃO
 modelo_ideb (BigQuery ML) ─EXPORT MODEL─► gs://<projeto>-modelos-ideb/modelo_ideb/v2/
                                             ├── modelo/  (TensorFlow SavedModel)  ─► Vertex AI Model Registry
                                             └── metadados_modelo.json                 (modelo-ideb, versão 2)
                                                   │
                                                   ▼ carregado na partida
                     Cloud Run: api-ideb (FastAPI) ◄── POST /predicao  (qualquer cliente HTTP, JSON)
                                    │              ◄── GET /escolas/{id}/predicao (lê a gold do lote e do streaming)
                                    └─ opcional: BACKEND=vertex consulta um Endpoint do Vertex AI
```

### Por que três códigos no streaming, e não cinco limpadores

**Um produtor só para as cinco tabelas.** Todas as mensagens vão para um único tópico, com o atributo `tabela` dizendo de onde vieram. Cinco produtores e cinco tópicos multiplicariam a infraestrutura sem nenhum ganho. Se um dia for preciso separar, o Pub/Sub filtra assinaturas por atributo sem mudar o produtor.

**Um serviço de limpeza, com uma função por tabela.** As regras ficam separadas por tabela dentro de `regras_limpeza.py` (`limpar_ideb`, `limpar_censo_escola` e assim por diante), o que dá a organização que um limpador por tabela daria, com um único deploy, uma única URL e um único ponto de escala.

**A gold em micro-lotes, não mensagem a mensagem.** As regras da fase 1 se dividem em dois tipos. As **sem estado** olham uma linha só (tipo, formato, faixa, filtro de etapa, COALESCE) e rodam no serviço de limpeza, em tempo real. As **com estado** precisam de várias linhas ao mesmo tempo (deduplicação, agregação dos docentes e junção das cinco fontes). Fazer essas a cada mensagem exigiria uma consulta ao BigQuery por mensagem, e cada consulta cobra no mínimo 10 MB por tabela lida. Por isso o código 3 roda um `MERGE` a cada 30 segundos e só promove uma escola quando ela está completa e nada novo chegou dela nos últimos 20 segundos (uma marca d'água simples). Dado atrasado atualiza a linha e a leva a `versao = 2`.

**Por que Cloud Run, e não Dataflow.** O serviço escala a zero, custa praticamente nada fora da demonstração e usa o mesmo Docker e o mesmo Cloud Run da API de predição.

## 3. Os dados do fluxo: registros reais de 2019 com ruído e defeitos controlados

O enunciado permite "um produtor/gerador de eventos simulando registros em tempo real". Os registros que fluem pelo Pub/Sub partem de dados reais e são descritos aqui com todas as letras, porque é assim que devem ser apresentados.

**A origem é real.** O gerador parte das 3.106 escolas reais de 2019 exportadas pela equipe do próprio BigQuery (`semente/`), com as linhas das cinco bronzes dessas escolas, todas do INEP via Base dos Dados. 2019 é o único ano completo da gold que o `modelo_ideb` não viu no treino. A amostragem é estratificada por região, quintil de INSE, rede e localização.

**O que o gerador altera, de propósito.** Cada escola do fluxo deriva de uma escola real de 2019, e sobre ela o gerador aplica:

- **código próprio:** UF + `99` + sequência (por exemplo `35990001`), para não se confundir com a escola real que já está na gold do lote;
- **ruído controlado:** proporcional nas matrículas e turmas, moderado no INSE (±0,04) e no IDEB (±0,15), nos indicadores (mantendo as somas que precisam dar 100) e em 2% das flags de infraestrutura;
- **corpo docente reconstruído:** linha a linha (docente x turma), reproduzindo as proporções reais da escola (superior, licenciatura, pós-graduação, efetivos, matemática e português). As demais colunas da bronze de docentes vêm de vínculos reais da amostra de 2019;
- **ano gravado 2023:** para simular a chegada de um ano novo (ver as limitações, seção 11);
- **defeitos controlados:** os problemas que a limpeza precisa resolver, um a um no `docs/CATALOGO_DEFEITOS.md` (40 códigos de regra). Cada linha afetada fica registrada em `dados/gabarito/manifesto_defeitos.csv`, com o valor limpo e o valor sujo.

UF, município, rede e localização são mantidos, o que preserva a distribuição regional e socioeconômica.

**Fidelidade conferida.** A distribuição por região e rede ficou praticamente idêntica à da semente. A correlação entre IDEB e INSE é 0,544 (0,546 na semente). A correlação de cada escola do fluxo com a escola real que lhe deu origem é de 0,987 no IDEB e de 0,994 no total de docentes.

**As regras valem para o dado real.** Aplicadas às linhas reais de IDEB, censo, indicadores e INSE das 3.106 escolas da semente, sem ruído nem defeitos, as nossas regras reproduzem a gold do lote da fase 1 nas 74 colunas que não vêm dos docentes, sem nenhuma diferença.

**Volume (padrão de 2.000 escolas):**

| Bronze | Mensagens | Aceitas | Descartadas (regra da fase 1) | Quarentena |
|---|---|---|---|---|
| `bronze_ideb_escola` | 4.121 | 1.947 | 2.146 | 28 |
| `bronze_censo_escola` | 2.038 | 2.029 | 0 | 9 |
| `bronze_indicadores_escola` | 2.031 | 2.027 | 0 | 4 |
| `bronze_inse_escola` | 2.068 | 1.995 | 55 | 18 |
| `bronze_censo_docente` | 58.702 | 47.698 | 10.769 | 235 |
| mensagens corrompidas | 118 | 0 | 0 | 118 |

São 69.078 mensagens, que resultam em 1.825 escolas na gold. A docente é bem maior porque o grão dela é docente x turma.

**Gerar os dados (obrigatório antes de usar o produtor ou os testes).** A pasta `dados/` não é versionada: a bronze de docentes sozinha passa de 100 MB, o limite do GitHub. O gerador é determinístico, então a mesma semente reproduz byte a byte os arquivos usados na implantação (cerca de 20 segundos):

```bash
pip install -r requirements-local.txt
python gerador/gerar_bronzes_fluxo.py --semente semente --saida dados --n-escolas 2000 --ano 2023
```

| Parâmetro | Efeito |
|---|---|
| `--ano` | ano gravado nas escolas do fluxo (`--ano 2019` elimina o deslocamento descrito na seção 11) |
| `--seed` | outra amostra e outro ruído |
| `--escala-defeitos 2` | dobra todas as taxas de defeito |
| `--vinculos-p` | número de turmas por docente |

## 4. Testes locais (sem GCP)

```bash
pip install -r requirements-local.txt
python testes/teste_ponta_a_ponta.py --dados dados      # pipeline de streaming
pip install -r api/requirements.txt httpx               # TensorFlow, FastAPI etc.
python testes/teste_api.py                              # API de predição
```

**Pipeline.** O teste lê as mensagens na ordem do produtor, monta o envelope do push do Pub/Sub e chama o próprio serviço Flask. Grava as silvers em memória, cria as views e a seleção da gold traduzindo o SQL do BigQuery para DuckDB e compara tudo com o gabarito, que vem de uma implementação independente das regras da fase 1 em pandas. Resultado: as 69.078 mensagens chegam ao destino esperado; os 11.313 defeitos do manifesto são detectados pela regra certa; a gold reconstruída tem as 1.825 linhas e as 87 colunas idênticas ao gabarito; os 18 comandos SQL são válidos; as colunas batem com o DDL de produção; e o MERGE incremental, em 16 micro-lotes, chega à mesma gold, com 7 escolas em `versao = 2`.

**API.** O teste monta um modelo no formato do `EXPORT MODEL` com os pesos reais do `modelo_ideb` (`semente/pesos_modelo.csv`, saída do `ML.WEIGHTS`) em três variantes de assinatura, porque a API lê a assinatura e se adapta a ela. Resultado: as 1.825 escolas da gold de streaming e as 3.106 escolas reais de 2019 têm a mesma previsão da referência (diferença máxima de 5×10⁻⁷, que é o arredondamento para 6 casas da resposta); a validação de entrada, os nulos, as categorias novas, o backend do Vertex AI (com um servidor local que imita o endpoint), a rota que lê a gold, o download do artefato do bucket e o `cliente_validacao.py` contra a API servida pelo uvicorn passam. São 36 verificações.

## 5. Implantação

A ordem é: pipeline de streaming (`infra/implantar.sh`), registro do modelo (`infra/registrar_modelo.sh`) e API (`infra/implantar_api.sh`). Cada script é idempotente e explica no topo o que faz. Os comandos completos, o tempo de cada passo e o que conferir depois estão no [MANUAL.md](MANUAL.md).

## 6. Roteiro da demonstração ao vivo

Resumo (o roteiro detalhado, com tempos e falas, está no MANUAL.md):

1. **Antes:** `limpar_ambiente.sh zerar`, iniciar o job `gold-ideb`, pré-carregar 1.700 escolas, deixar a API com uma instância ligada.
2. **Streaming ao vivo:** publicar 300 escolas a 40 mensagens por segundo e acompanhar as consultas de `sql/03_monitoramento_demo.sql` (chegada, placar da limpeza, regras, quarentena, antes e depois, funil, lote e streaming lado a lado, dados tardios).
3. **Modelo e API ao vivo:** o modelo no Vertex AI Model Registry, a página `/docs`, um `curl` com uma escola, uma entrada inválida, e `GET /escolas/{id}/predicao` para uma escola que acabou de chegar pelo fluxo, com a previsão da API igual à do BigQuery.
4. **Fechamento:** `api/cliente_validacao.py` (concordância e latência) e `sql/04_avaliacao_modelo.sql`.

## 7. A relação entre lote e streaming no BigQuery

O pipeline em lote está em [`lote/`](lote/README.md): guarda a história (2013, 2015 e 2019) e treina o modelo. O streaming mantém a base viva com dado novo limpo em segundos.

Cada camada de streaming espelha uma camada do lote com os mesmos nomes de colunas, mais metadados de linhagem (`_message_id`, `_publish_time`, `_ingestao_ts` e `_regras_aplicadas`).

| Camada | Lote (fase 1) | Streaming (fase 2) |
|---|---|---|
| Bronze | `bronze_*` (cópias da Base dos Dados) | `bronze_stream_eventos` (JSON cru, gravado pela assinatura BigQuery) e, opcionalmente, `bronze_fluxo_*` |
| Silver | `silver_ideb_escola`, `silver_censo_escola`, `silver_indicadores_escola`, `silver_inse_escola`, `silver_censo_docente_agregado` | `silver_stream_*` (append-only, partição diária por ingestão) e as views `vw_silver_stream_*` com a deduplicação e a agregação |
| Gold | `gold_ideb_features` | `gold_ideb_features_stream` (mesmas 87 colunas, com `versao`) |
| União | | `vw_gold_ideb_unificada`, com a coluna `origem_dado` ('lote' ou 'streaming') |
| Predição | `ML.PREDICT` sob demanda | `predicoes_ideb_stream` (a cada ciclo do job) e a API |

## 8. O modelo em produção: artefato e Vertex AI Model Registry

O `modelo_ideb` é uma regressão linear do BigQuery ML treinada em 2013 e 2015. Há três avaliações diferentes, que não devem ser confundidas:

| Avaliação | MAE | R² |
|---|---:|---:|
| Interna do modelo (`ML.EVALUATE` sem fornecer dados) | 0,467 | 0,532 |
| Dados de 2019 fornecidos explicitamente | 0,558 | 0,376 |
| Escolas do fluxo de streaming | 0,542 | 0,449 |

 Dentro do BigQuery ele só é usado por SQL. O `infra/registrar_modelo.sh` o leva para fora em três passos:

1. **Artefato pronto para produção.** `EXPORT MODEL` grava o modelo no Cloud Storage em `gs://<projeto>-modelos-ideb/modelo_ideb/v1/modelo/`, no formato TensorFlow SavedModel. Esse formato leva junto o pré-processamento que o BigQuery ML fazia sozinho, como a codificação das seis colunas de texto (UF, município, rede, localização, localização diferenciada e ICG).
2. **Metadados.** Ao lado fica o `metadados_modelo.json`, montado com `ML.FEATURE_INFO`, `ML.WEIGHTS` e `ML.EVALUATE`: as médias de treino (para preencher nulos como o `ML.PREDICT` faz), as categorias vistas no treino (para avisar quando chega uma nova) e as métricas oficiais de 2019.
3. **Registro.** `gcloud ai models upload` registra o artefato no Vertex AI Model Registry como `modelo-ideb`, versão `v1` (alias), com rótulos, descrição com as métricas e o contêiner oficial de predição do TensorFlow, o que deixa o modelo pronto para ser implantado num endpoint com um comando. Uma versão nova é só `VERSAO=<nova> bash infra/registrar_modelo.sh`; o histórico fica no registro.

**A versão em produção é a v2.** A validação da API contra o `ML.PREDICT` mostrou que o artefato v1 trazia dois blocos de pesos trocados (`tipo_localizacao_diferenciada` antes de `tipo_localizacao`): 0 de 300 escolas dentro da tolerância (`evidencias/historico/`). A cópia corrigida foi publicada como v2, sem retreino e sem alterar o modelo no BigQuery ML, e bate em 10.000 de 10.000 registros. O procedimento está em `docs/NOTAS_DA_IMPLANTACAO.md`. Não rode `registrar_modelo.sh` sobre a v2: ele exporta de novo do BigQuery e regeneraria o artefato sem a correção.

O bucket é criado na mesma localização do dataset (região `us-central1` quando o dataset está na multirregião US), para atender às regras de localização do `EXPORT MODEL` e do Vertex AI.

## 9. A API REST de predição (`api/`)

**Contêiner e Cloud Run.** O `Dockerfile` monta uma imagem Python 3.12 com FastAPI, uvicorn e TensorFlow. O Cloud Build a constrói, o Artifact Registry a guarda e o Cloud Run a executa com uma conta de serviço própria (`sa-api-ideb`, só leitura no bucket e no BigQuery). O modelo não vai dentro da imagem: a API baixa o artefato de `MODELO_URI` ao subir, então trocar de versão é mudar uma variável.

**Rotas.**

| Rota | O que faz |
|---|---|
| `POST /predicao` | recebe de 1 a 1.000 escolas, cada uma com as 85 features (as 87 colunas da gold menos `id_escola` e `ideb`), e devolve o IDEB previsto em JSON |
| `GET /escolas/{id}/predicao` | busca a escola na `vw_gold_ideb_unificada` (lote e streaming) e prevê na hora, mostrando ao lado a predição que o BigQuery gravou |
| `GET /modelo` | versão, origem do artefato, métricas oficiais, assinatura do modelo e as 85 features |
| `GET /saude` | 200 com o modelo carregado, 503 se não carregou |
| `GET /docs` | documentação interativa gerada automaticamente, com exemplo pronto |

**Exemplo.**

```bash
curl -X POST "$URL_API/predicao" -H "Content-Type: application/json" -d @api/exemplos/uma_escola.json
```

```json
{
  "modelo": {"nome": "modelo_ideb", "versao": "v2", "backend": "savedmodel"},
  "quantidade": 1,
  "predicoes": [{"indice": 0, "id_escola": "11990001", "ideb_previsto": 4.643232,
                 "ideb_informado": 4.4, "erro_absoluto": 0.243232, "avisos": []}],
  "latencia_ms": 4.3
}
```

**O que a API faz com a entrada.** Antes de prever, a API valida tipos, campos faltando e campos desconhecidos (erro 422 com a lista de problemas). Depois aplica as mesmas convenções da limpeza e do BigQuery ML, para que a previsão servida seja a mesma do `ML.PREDICT`: normaliza as categorias ("estadual" vira "2", "Nível 3" vira "nivel 3", "0" vira "0.0"), preenche número nulo com a média de treino e trata texto nulo ou categoria nunca vista como peso zero. Cada caso desses gera um aviso na resposta.

**Dois jeitos de rodar a inferência**, escolhidos por `BACKEND`:

| | `savedmodel` (padrão) | `vertex` (opcional) |
|---|---|---|
| Onde roda o modelo | dentro da API, em memória | num Endpoint do Vertex AI (`infra/endpoint_vertex.sh`) |
| Custo parado | zero (o Cloud Run escala a zero) | cobra por hora de nó implantado, mesmo sem requisições (US$ 0,1095 por hora no n1-standard-2 em us-central1) |
| Latência | milissegundos dentro da API | soma uma chamada de rede ao endpoint |
| Para que serve | operação normal | mostrar a segunda forma pedida no enunciado |

**Painel ao vivo.** `python api/monitor_ao_vivo.py --url $URL_API` atualiza a tela a cada 3 segundos com as mensagens do último minuto, as escolas que o job acabou de consolidar na gold e a previsão da API para cada uma, ao lado da previsão do BigQuery.

**Validação em produção.** `api/cliente_validacao.py` pega escolas da gold, envia à API e compara com o `ML.PREDICT` do BigQuery (as duas precisam coincidir até a terceira casa decimal), mede a latência e testa a rota de leitura da gold.

**Logs.** Cada requisição gera uma linha JSON no Cloud Logging, com rota, quantidade de escolas, latência e backend.

## 10. Custos esperados

| Serviço | Uso estimado numa demonstração de uma hora |
|---|---|
| Pub/Sub | cerca de 0,3 GB, para 10 GB gratuitos por mês |
| Cloud Run (limpeza e API) | cerca de 10 mil vCPU-segundos, para 180 mil gratuitos por mês |
| Inserções por streaming no BigQuery | cerca de 140 MB, menos de US$ 0,01 |
| MERGEs do job da gold | de 10 a 15 GB processados por hora, para 1 TB gratuito por mês |
| Consultas da API e da validação | cada consulta cobra no mínimo 10 MB; algumas centenas ficam em poucos GB |
| Artefato no Cloud Storage e registro no Vertex AI | poucos MB; o registro não cobra |
| Imagens no Artifact Registry | cerca de 1 GB da imagem da API, centavos por mês |
| Endpoint do Vertex AI (opcional) | US$ 0,1095 por hora de nó enquanto estiver implantado |

Os valores são estimativas, não medições de fatura. O serviço de limpeza e a API escalam a zero; o job da gold para sozinho depois de 55 minutos; o endpoint precisa ser removido à mão (`infra/endpoint_vertex.sh remover`).

## 11. Decisões e limitações (para apresentar com transparência)

1. **Os dados do fluxo derivam de escolas reais de 2019, mas não são registros coletados em 2023.** Têm ruído, defeitos controlados, código próprio e corpo docente reconstruído, como descreve a seção 3. A apresentação deve dizer isso.
2. **O ano gravado desloca a predição.** As escolas do fluxo carregam `ano = 2023`, mas o conteúdo vem de 2019. `ano` é uma feature numérica do modelo linear, com peso de 0,066 por ano, então rotular como 2023 soma 4 × 0,066 = **0,26 ponto a toda predição**, em relação a 2019. A consulta 3 de `sql/04` mede isso. É um exemplo de drift causado por uma feature temporal. Para eliminar o efeito, gere com `--ano 2019`.
3. **Herança da fase 1: colunas do censo que mudaram entre os anos.** Na base de 2019, oito atributos da gold vêm sempre nulos da Base dos Dados e viram 0 (por exemplo `quantidade_funcionario`, que tinha média 60 no treino). Na direção oposta, doze colunas constantes no treino variam em 2019 e têm peso zero no modelo. O streaming reproduz isso, porque o dado é de 2019.
4. **Agregado de docentes reconstruído.** Nomes e tipos vêm do DDL de produção; as expressões foram inferidas das estatísticas reais (docentes distintos, etapas 19, 20, 21 e 41, superior = `'4'`, efetivo = `'1'`, média de idade com uma casa). O SQL original não foi recuperado. O ajuste, se preciso, é numa lista de `pipeline/esquema.py`.
5. **Turmas por docente reduzidas.** Cada docente do fluxo tem em média 1,35 turma nos anos finais, contra 4 ou 5 na vida real, para manter o volume demonstrável. As contagens de docentes distintos não mudam.
6. **A avaliação no streaming não é um teste novo de generalização.** As escolas do fluxo derivam de escolas de 2019, então as métricas tendem a repetir a avaliação de 2019 da fase 1, mais o deslocamento do item 2. O valor está em mostrar o fluxo e o modelo operando com dados que chegam continuamente.
7. **Garantias do streaming.** O Pub/Sub entrega pelo menos uma vez. O serviço usa o `messageId` como `insertId` no BigQuery e as views resolvem o restante pela chave de negócio. Mensagens que falham 5 vezes vão para a dead letter.
8. **API: nulos e categorias.** Para número nulo a API usa a média de treino, que é a regra do `ML.PREDICT`. Para texto nulo ou categoria nunca vista, envia uma categoria vazia, que o modelo trata como peso zero. O `cliente_validacao.py` mostra quantas escolas tiveram avisos e se a previsão continuou igual à do BigQuery.
9. **API: partida a frio e acesso.** Com zero instâncias, a primeira requisição espera a instância subir e carregar o TensorFlow (cerca de 10 a 20 segundos). No dia da apresentação, use uma instância mínima. A API fica aberta por padrão para a demonstração com `curl` e `/docs`; `PUBLICA=nao` exige autenticação.
