-- Modelo secundário: árvore boosted (não-linear), pra comparar contra a
-- regressão linear. Mesmo split temporal (treino ano < 2019).
--
-- Experimento comparativo, fora da produção: o modelo servido no Vertex AI
-- e na API é o linear (modelo_ideb), que exporta como TensorFlow SavedModel
-- e é reproduzido exatamente pela API. Treino: ~22 min, ~49 GB processados.

CREATE OR REPLACE MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb_boosted`
OPTIONS(model_type = 'BOOSTED_TREE_REGRESSOR', input_label_cols = ['ideb']) AS
SELECT
  * EXCEPT(id_escola)
FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features`
WHERE ano < 2019;

-- Avaliação no mesmo ano nunca visto (2019), comparável à do linear:
SELECT *
FROM ML.EVALUATE(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb_boosted`,
  (SELECT * EXCEPT(id_escola) FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features` WHERE ano = 2019));
