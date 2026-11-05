# Notas da implantação e validação

## Ambiente
- Projeto: platinum-avenue-505512-t5
- Dataset: trabalho_pdm
- Região dos serviços: us-central1
- API: https://api-ideb-1071424267162.us-central1.run.app
- Documentação interativa: https://api-ideb-1071424267162.us-central1.run.app/docs
- Modelo servido: v2, backend TensorFlow SavedModel no Cloud Run.
- Modelo registrado no Vertex AI: 9113764471460855808, versão 2.
- O endpoint opcional de predição do Vertex AI não foi implantado.

## Correções realizadas
1. Leitura dos arquivos SQL pela entrada padrão do comando bq,
   evitando interpretar comentários iniciais como opções.
2. Uso do caminho completo do modelo em --parent-model no registro
   de novas versões no Vertex AI.
3. Reordenação de dois blocos de pesos no SavedModel exportado:
   tipo_localizacao_diferenciada antes de tipo_localizacao.

A terceira correção foi específica ao artefato v1 diagnosticado.
O modelo original foi preservado e a cópia corrigida publicada como v2.
Não houve novo treinamento nem alteração do modelo no BigQuery ML.

## Procedimento de reprodução da correção
O script modelo/corrigir_exportacao_v2.py recebe uma pasta contendo
modelo/ e metadados_modelo.json. A pasta diagnostico/ não é versionada;
baixe o artefato v1 do Cloud Storage antes:

    gcloud storage cp -r gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v1 diagnostico/

 Ele verifica a versão, o formato
e os pesos esperados antes de criar uma cópia corrigida.

Exemplo, a partir da raiz do projeto, com o ambiente virtual ativo:

    python modelo/corrigir_exportacao_v2.py \
      --origem diagnostico/v1 \
      --destino diagnostico/v2_reproduzida \
      --uri-artefato gs://platinum-avenue-505512-t5-modelos-ideb/modelo_ideb/v2/modelo

O destino deve ser uma pasta nova. Esse comando não publica o modelo.
Toda cópia reproduzida deve ser validada contra o BigQuery ML antes
de ser publicada. O arquivo do script teve a sintaxe verificada;
a correção equivalente executada durante o diagnóstico foi validada
nos conjuntos descritos abaixo.

Não executar registrar_modelo.sh sobre a v2 corrigida sem revisar
o fluxo: o script original de exportação regenera os metadados e
pode sobrescrever o registro da correção.

## Validação
- Cópia corrigida: 10.000 registros de 2019 e 17 do streaming
  dentro da tolerância de 0,001.
- API v2: validação em lote aprovada na amostra de 300 registros.
- API v2: 1.825 de 1.825 escolas do streaming dentro da tolerância.
- Diferença máxima API x BigQuery ML no streaming: 5,64e-7.
- Bronze: 69.078 registros.
- Auditoria: 69.078 registros.
- Gold streaming: 1.825 registros.
- Predições streaming: 1.825 registros.

Os resultados detalhados estão na pasta evidencias/.

## Interpretação correta das métricas
Os valores abaixo correspondem a avaliações diferentes:

| Avaliação | MAE | R² |
|---|---:|---:|
| Interna do modelo, ML.EVALUATE sem fornecer dados | 0,4669 | 0,5316 |
| Dados de 2019 fornecidos explicitamente | 0,5579 | 0,3762 |
| Escolas do fluxo simulado | 0,5420 | 0,4492 |

Retificação: os valores aproximados MAE 0,467 e R² 0,532 são da
avaliação interna do modelo, não da avaliação explícita de 2019.

A concordância API x BigQuery verifica a reprodução das previsões.
Ela não significa ausência de erro em relação ao IDEB observado.

## Dados e ensaio
Os dados do streaming são simulados a partir de dados de 2019,
com alterações e ano marcado como 2023. Não são observações reais
coletadas em 2023, nem um conjunto independente de generalização.

O envio foi realizado em duas etapas: 20 escolas no teste inicial
e as 1.980 restantes depois, totalizando 69.078 mensagens.
Isso adapta o roteiro de envio do manual. O ensaio cronometrado
da apresentação ainda deve ser realizado pela equipe.
