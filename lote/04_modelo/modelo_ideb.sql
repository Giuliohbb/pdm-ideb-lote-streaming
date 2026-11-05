-- Modelo principal: regressão linear (BigQuery ML), 100% SQL.
-- Alvo (ideb) é contínuo → regressão, não classificação.
--
-- Split temporal (não aleatório): treina em anos < 2019, avalia em 2019.
-- Escolha metodológica: evita vazamento de informação entre anos e
-- simula o cenário real de uso (prever IDEB de um ano ainda não visto).
-- Na prática a Gold só tem 2013, 2015 e 2019 (interseção das 5 fontes),
-- então o treino usa 2013 e 2015 e 2019 fica como ano nunca visto.

CREATE OR REPLACE MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`
OPTIONS(model_type = 'linear_reg', input_label_cols = ['ideb']) AS
SELECT
  * EXCEPT(id_escola)
FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features`
WHERE ano < 2019;

-- Avaliação em dado nunca visto pelo modelo (ano = 2019).
-- Resultado desta consulta (2019 fornecido explicitamente):
--   MAE = 0,558 | R² = 0,376
-- Atenção: MAE 0,467 e R² 0,532 são da avaliação INTERNA do modelo
-- (ML.EVALUATE sem fornecer dados, sobre o split automático do treino),
-- não de 2019. Evidências em evidencias/avaliacao_modelo.txt e
-- evidencias/evidencia_avaliacao_interna.txt.
SELECT *
FROM ML.EVALUATE(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
  (SELECT * EXCEPT(id_escola) FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features` WHERE ano = 2019));

-- Predição individual — útil pra demo ao vivo: mostra escolas onde o
-- modelo acertou de perto (ordenado por menor erro absoluto).
SELECT
  id_escola,
  ideb AS ideb_real,
  ROUND(predicted_ideb, 2) AS ideb_previsto,
  ROUND(ABS(ideb - predicted_ideb), 2) AS erro_absoluto
FROM ML.PREDICT(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`,
  (SELECT * FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features` WHERE ano = 2019))
ORDER BY erro_absoluto ASC
LIMIT 20;
