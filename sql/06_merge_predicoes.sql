-- Arquivo gerado por pipeline/gerar_sql.py a partir de pipeline/esquema.py. Não edite à mão.
-- Predição com o modelo em produção para as versões da gold ainda não preditas.

MERGE `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream` AS T
USING (
  SELECT id_escola, ano, versao AS versao_gold, ideb AS ideb_real, predicted_ideb AS ideb_previsto
  FROM ML.PREDICT(MODEL `platinum-avenue-505512-t5.trabalho_pdm.modelo_ideb`, (
    SELECT g.*
    FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream` AS g
    LEFT JOIN `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream` AS p ON p.id_escola = g.id_escola AND p.ano = g.ano
    WHERE p.id_escola IS NULL OR p.versao_gold < g.versao
  ))
) AS S
ON T.id_escola = S.id_escola AND T.ano = S.ano
WHEN MATCHED THEN UPDATE SET
    versao_gold = S.versao_gold, ideb_real = S.ideb_real, ideb_previsto = S.ideb_previsto,
    erro_absoluto = ABS(S.ideb_real - S.ideb_previsto), modelo = 'modelo_ideb', previsto_em = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (id_escola, ano, versao_gold, ideb_real, ideb_previsto, erro_absoluto, modelo, previsto_em)
VALUES (S.id_escola, S.ano, S.versao_gold, S.ideb_real, S.ideb_previsto, ABS(S.ideb_real - S.ideb_previsto),
        'modelo_ideb', CURRENT_TIMESTAMP());
