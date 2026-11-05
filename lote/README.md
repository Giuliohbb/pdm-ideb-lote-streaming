# Pipeline em lote (Trabalho 1)

Arquitetura medalhão 100% SQL no BigQuery. Constrói a base histórica `gold_ideb_features` (escola x ano, 87 colunas) e treina o `modelo_ideb`, o mesmo modelo que o Trabalho 2 leva para o Vertex AI e serve na API.

```
01_bronze  ->  02_silver  ->  03_gold  ->  04_modelo
(cópia fiel)   (limpo)        (junção)     (BigQuery ML)
```

## Papel do lote na arquitetura híbrida

| | Lote | Streaming (`../pipeline`) |
|---|---|---|
| Fonte | tabelas públicas do INEP na Base dos Dados | eventos JSON no Pub/Sub |
| Frequência | sob demanda, quando sai uma nova edição do INEP | contínua, gold atualizada a cada 30 s |
| Volume | `bronze_censo_docente` com ~134 milhões de linhas | 69.078 mensagens na demonstração |
| Resultado | gold histórica: 2013, 2015 e 2019 | gold de streaming com as mesmas 87 colunas |
| Papel | guarda a história e treina o modelo | mantém a base viva com dado novo limpo em segundos |

As duas golds se encontram na view `vw_gold_ideb_unificada` (`../sql/02_views.sql`), com a coluna `origem_dado`.

## Como executar

No console do BigQuery ou com `bq query --use_legacy_sql=false < arquivo.sql`, nesta ordem:

1. `01_bronze/*.sql`: cópias das cinco fontes (IDEB, censo escola, censo docente, indicadores e INSE).
2. `02_silver/*.sql`: limpeza, tipagem e deduplicação por `id_escola + ano`, uma tabela por fonte.
3. `03_gold/gold_ideb_features.sql`: junção das cinco silvers.
4. `04_modelo/modelo_ideb.sql`: treino em 2013 e 2015, avaliação em 2019 e predições de exemplo.

Todos os scripts usam o projeto `platinum-avenue-505512-t5` e o dataset `trabalho_pdm`.

## Decisões principais

- **Sem vazamento no alvo.** Taxa de aprovação, indicador de rendimento e notas do SAEB compõem a própria fórmula do IDEB e ficaram de fora das features.
- **IDEB nulo descartado (~38% das linhas).** O INEP suprime o cálculo em turmas pequenas, o que pode concentrar a perda em escolas pequenas ou rurais (possível viés de seleção).
- **INSE só com a média.** Os percentuais por nível são nulos nos anos antigos, então ficou só `inse`, priorizando a cobertura temporal.
- **Censo de 455 para cerca de 62 colunas**, cortadas por bloco temático. Nulo vira 0, porque no censo declaratório a ausência significa "não possui".
- **Gold com INNER JOIN nas cinco fontes.** A tabela fica sem nulos, ao custo de cobertura: das 311.340 linhas de IDEB sobram 84.998, só dos anos em que as cinco fontes existem.
- **Split temporal.** O modelo treina nos anos anteriores a 2019 e avalia em 2019, simulando a previsão de um ano ainda não visto.

## Métricas do `modelo_ideb`

| Avaliação | MAE | R² |
|---|---:|---:|
| Interna do modelo (`ML.EVALUATE` sem fornecer dados) | 0,467 | 0,532 |
| 2019 fornecido explicitamente | 0,558 | 0,376 |

O `modelo_ideb_boosted` é um experimento comparativo e não está em produção.

## Procedência

Os scripts são os entregues no Trabalho 1, com duas mudanças: `CREATE OR REPLACE` nas silvers, para que possam ser executados de novo, e a `silver_censo_docente_agregado`, cujo SQL original não ficou no histórico de jobs. Ela foi reconstruída com os nomes e tipos da tabela em produção. O cabeçalho do arquivo explica como.
