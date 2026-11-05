-- =============================================================================
-- Consultas para a demonstração ao vivo do streaming (rode uma por vez no console).
-- Projeto platinum-avenue-505512-t5, dataset trabalho_pdm.
-- =============================================================================


-- 1. O fluxo chegando: mensagens por minuto, tabela e destino (rode várias vezes durante a demo)
SELECT
  TIMESTAMP_TRUNC(processado_em, MINUTE) AS minuto,
  tabela,
  status,
  COUNT(*) AS mensagens
FROM `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream`
WHERE processado_em >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY minuto, tabela, status
ORDER BY minuto DESC, tabela, status;


-- 2. Placar da limpeza por tabela
SELECT
  tabela,
  COUNT(*) AS recebidas,
  COUNTIF(status = 'aceito') AS aceitas,
  COUNTIF(status = 'aceito' AND ARRAY_LENGTH(regras_aplicadas) > 0) AS aceitas_com_correcao,
  COUNTIF(status = 'descartado') AS descartadas_regra_fase1,
  COUNTIF(status = 'quarentena') AS quarentena
FROM `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream`
GROUP BY tabela
ORDER BY tabela;


-- 3. O que o pipeline está corrigindo: regras aplicadas (códigos do catálogo de defeitos)
SELECT
  REGEXP_EXTRACT(regra, r'^([A-Z]+[0-9]+)') AS codigo,
  tabela,
  COUNT(*) AS mensagens
FROM `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream`,
  UNNEST(regras_aplicadas) AS regra
GROUP BY codigo, tabela
ORDER BY mensagens DESC;


-- 4. Quarentena: o que foi barrado e por quê (com o payload cru)
SELECT processado_em, tabela, ref, motivo, SUBSTR(payload, 1, 160) AS trecho_do_payload
FROM `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream`
WHERE status = 'quarentena'
ORDER BY processado_em DESC
LIMIT 30;


-- 5. Antes e depois de UM registro: bronze crua x decisão da limpeza x silver limpa.
--    Escolha um ref no arquivo dados/gabarito/manifesto_defeitos.csv (colunas tabela e linha),
--    por exemplo 'ideb:12' ou 'censo_escola:40'.
DECLARE alvo STRING DEFAULT 'ideb:12';
SELECT
  b.publish_time,
  b.data AS registro_bruto,
  a.status,
  a.regras_aplicadas,
  a.motivo,
  TO_JSON_STRING(s) AS registro_limpo_ideb
FROM `platinum-avenue-505512-t5.trabalho_pdm.vw_bronze_stream_eventos` AS b
JOIN `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream` AS a USING (message_id)
LEFT JOIN `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_ideb_escola` AS s ON s._message_id = b.message_id
WHERE b.ref = alvo;


-- 6. Funil por camada (bronze crua -> silvers -> gold -> predições)
SELECT '1 bronze crua (mensagens)' AS camada, COUNT(*) AS linhas FROM `platinum-avenue-505512-t5.trabalho_pdm.bronze_stream_eventos`
UNION ALL SELECT '2 silver ideb', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_ideb_escola`
UNION ALL SELECT '2 silver censo escola', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_censo_escola`
UNION ALL SELECT '2 silver indicadores', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_indicadores_escola`
UNION ALL SELECT '2 silver inse', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_inse_escola`
UNION ALL SELECT '2 silver docente (vinculos)', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_censo_docente`
UNION ALL SELECT '3 gold streaming (escolas)', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`
UNION ALL SELECT '4 predicoes', COUNT(*) FROM `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream`
ORDER BY camada;


-- 7. Latência entre a publicação no Pub/Sub e a gravação na silver (mediana e p95)
SELECT
  tabela,
  COUNT(*) AS mensagens,
  APPROX_QUANTILES(TIMESTAMP_DIFF(processado_em, publish_time, MILLISECOND), 100)[OFFSET(50)] AS p50_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(processado_em, publish_time, MILLISECOND), 100)[OFFSET(95)] AS p95_ms
FROM `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream`
WHERE processado_em >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY tabela;


-- 8. Lote x streaming no BigQuery: partições novas das tabelas de streaming
SELECT table_name, partition_id, total_rows, last_modified_time
FROM `platinum-avenue-505512-t5.trabalho_pdm.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name LIKE '%stream%'
ORDER BY table_name, partition_id;


-- 9. Lote x streaming lado a lado na view unificada (histórico real + dados novos)
SELECT
  origem_dado,
  ano,
  COUNT(*) AS escolas,
  ROUND(AVG(ideb), 2) AS ideb_medio,
  ROUND(AVG(inse), 2) AS inse_medio,
  ROUND(AVG(qtd_docente_anos_finais), 1) AS docentes_medio
FROM `platinum-avenue-505512-t5.trabalho_pdm.vw_gold_ideb_unificada`
GROUP BY origem_dado, ano
ORDER BY ano, origem_dado;


-- 10. Dados tardios: escolas cuja gold foi atualizada depois da primeira carga
SELECT id_escola, versao, primeira_carga_em, atualizado_em, qtd_docente_anos_finais
FROM `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream`
WHERE versao > 1
ORDER BY atualizado_em DESC;


-- 11. Duplicatas resolvidas na consolidação (linhas recebidas x linhas únicas)
SELECT 'ideb' AS tabela, COUNT(*) AS linhas_silver, COUNT(DISTINCT CONCAT(id_escola, '-', CAST(ano AS STRING))) AS chaves_unicas
FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_ideb_escola`
UNION ALL
SELECT 'inse', COUNT(*), COUNT(DISTINCT CONCAT(id_escola, '-', CAST(ano AS STRING)))
FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_inse_escola`
UNION ALL
SELECT 'docente (vinculos)', COUNT(*), COUNT(DISTINCT CONCAT(id_escola, id_docente, IFNULL(id_turma, '')))
FROM `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_censo_docente`;


-- 12. Dead letter: mensagens que falharam 5 vezes (deve ficar vazio). No Cloud Shell:
--     gcloud pubsub subscriptions pull sub-dlq-inspecao --limit=10 --auto-ack
