-- =============================================================================
-- Avaliação do modelo em produção sobre os dados que chegaram pelo streaming.
-- O modelo_ideb (regressão linear da fase 1) foi treinado em 2013 e 2015.
-- =============================================================================


-- 1. Métricas: avaliação da fase 1 (gold de 2019) x gold de streaming (registros de 2019 com ruído, rotulados 2023)
SELECT 'fase 1: 2019 real' AS conjunto, *
FROM ML.EVALUATE(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
  (SELECT * EXCEPT (id_escola) FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features` WHERE ano = 2019))
UNION ALL
SELECT 'streaming: escolas do fluxo' AS conjunto, *
FROM ML.EVALUATE(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
  (SELECT * EXCEPT (id_escola, origem, primeira_carga_em, atualizado_em, versao, _hash)
   FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`));


-- 2. Erro e viés por região e rede (viés > 0 = o modelo superestima)
SELECT
  CASE
    WHEN g.sigla_uf IN ('AC','AM','AP','PA','RO','RR','TO') THEN 'Norte'
    WHEN g.sigla_uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'Nordeste'
    WHEN g.sigla_uf IN ('ES','MG','RJ','SP') THEN 'Sudeste'
    WHEN g.sigla_uf IN ('PR','RS','SC') THEN 'Sul'
    ELSE 'Centro-Oeste'
  END AS regiao,
  g.rede,
  COUNT(*) AS escolas,
  ROUND(AVG(p.erro_absoluto), 3) AS mae,
  ROUND(AVG(p.ideb_previsto - p.ideb_real), 3) AS vies
FROM `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream` AS p
JOIN `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream` AS g USING (id_escola, ano)
GROUP BY regiao, g.rede
ORDER BY regiao, g.rede;


-- 3. Efeito do campo `ano` (feature numérica do modelo): mesmas escolas, ano 2023 x 2019.
--    O peso de `ano` no modelo_ideb é 0,066 por ano, então a diferença esperada é 4 x 0,066 = 0,26.
WITH p2023 AS (
  SELECT id_escola, predicted_ideb
  FROM ML.PREDICT(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
    (SELECT * FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`))
),
p2019 AS (
  SELECT id_escola, predicted_ideb
  FROM ML.PREDICT(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
    (SELECT * REPLACE (2019 AS ano) FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`))
)
SELECT ROUND(AVG(a.predicted_ideb - b.predicted_ideb), 3) AS deslocamento_medio_por_usar_2023
FROM p2023 AS a JOIN p2019 AS b USING (id_escola);


-- 4. As 20 escolas com maior erro (para discutir limitações na apresentação)
SELECT p.id_escola, g.sigla_uf, g.rede, g.tipo_localizacao, g.inse, p.ideb_real,
       ROUND(p.ideb_previsto, 2) AS ideb_previsto, ROUND(p.erro_absoluto, 2) AS erro
FROM `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream` AS p
JOIN `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream` AS g USING (id_escola, ano)
ORDER BY p.erro_absoluto DESC
LIMIT 20;
