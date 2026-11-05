-- Arquivo gerado por pipeline/gerar_sql.py a partir de pipeline/esquema.py. Não edite à mão.
-- Tabelas de streaming: bronze crua, 5 silvers, auditoria, gold e predições.

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.bronze_stream_eventos` (
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  data STRING,
  attributes STRING
)
PARTITION BY DATE(publish_time)
OPTIONS (description = 'Bronze de streaming: mensagens cruas do Pub/Sub gravadas pela assinatura BigQuery (sem codigo).');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_ideb_escola` (
  id_escola STRING,
  ano INT64,
  ideb FLOAT64,
  _message_id STRING,
  _publish_time TIMESTAMP,
  _ingestao_ts TIMESTAMP,
  _regras_aplicadas ARRAY<STRING>
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = 'Silver de streaming do IDEB (append-only). Mesmas colunas da silver_ideb_escola + metadados.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_censo_escola` (
  id_escola STRING,
  ano INT64,
  sigla_uf STRING,
  id_municipio STRING,
  rede STRING,
  tipo_localizacao STRING,
  tipo_localizacao_diferenciada STRING,
  local_funcionamento_predio_escolar INT64,
  predio_compartilhado INT64,
  agua_potavel INT64,
  energia_rede_publica INT64,
  esgoto_rede_publica INT64,
  lixo_servico_coleta INT64,
  biblioteca INT64,
  laboratorio_ciencias INT64,
  laboratorio_informatica INT64,
  quadra_esportes INT64,
  patio_coberto INT64,
  refeitorio INT64,
  cozinha INT64,
  sala_multiuso INT64,
  banheiro INT64,
  banheiro_pne INT64,
  area_verde INT64,
  auditorio INT64,
  sala_atendimento_especial INT64,
  dependencia_pne INT64,
  acessibilidade_inexistente INT64,
  quantidade_sala_existente INT64,
  quantidade_sala_utilizada_climatizada INT64,
  equipamento_computador INT64,
  equipamento_lousa_digital INT64,
  equipamento_multimidia INT64,
  quantidade_computador_aluno INT64,
  quantidade_computador INT64,
  internet INT64,
  internet_alunos INT64,
  banda_larga INT64,
  quantidade_funcionario INT64,
  profissional_coordenador INT64,
  profissional_psicologo INT64,
  profissional_assistente_social INT64,
  profissional_bibliotecario INT64,
  alimentacao INT64,
  exame_selecao INT64,
  diurno INT64,
  noturno INT64,
  etapa_ensino_fundamental_anos_iniciais INT64,
  etapa_ensino_medio INT64,
  etapa_ensino_eja INT64,
  etapa_ensino_especial INT64,
  qtd_matricula_anos_finais INT64,
  qtd_matricula_fundamental INT64,
  qtd_matricula_feminino INT64,
  qtd_matricula_masculino INT64,
  qtd_matricula_branca INT64,
  qtd_matricula_preta INT64,
  qtd_matricula_parda INT64,
  qtd_matricula_indigena INT64,
  qtd_matricula_zona_rural INT64,
  qtd_matricula_transporte_publico INT64,
  qtd_docente_anos_finais INT64,
  qtd_docente_educacao_basica INT64,
  qtd_turma_anos_finais INT64,
  _message_id STRING,
  _publish_time TIMESTAMP,
  _ingestao_ts TIMESTAMP,
  _regras_aplicadas ARRAY<STRING>
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = 'Silver de streaming do Censo Escola (append-only). Mesmas 64 colunas da silver_censo_escola + metadados.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_indicadores_escola` (
  id_escola STRING,
  ano INT64,
  atu_ef_anos_finais FLOAT64,
  had_ef_anos_finais FLOAT64,
  afd_ef_anos_finais_grupo_1 FLOAT64,
  ird_media_regularidade_docente FLOAT64,
  ied_ef_anos_finais_nivel_1 FLOAT64,
  ied_ef_anos_finais_nivel_2 FLOAT64,
  ied_ef_anos_finais_nivel_3 FLOAT64,
  ied_ef_anos_finais_nivel_4 FLOAT64,
  ied_ef_anos_finais_nivel_5 FLOAT64,
  ied_ef_anos_finais_nivel_6 FLOAT64,
  icg_nivel_complexidade_gestao_escola STRING,
  _message_id STRING,
  _publish_time TIMESTAMP,
  _ingestao_ts TIMESTAMP,
  _regras_aplicadas ARRAY<STRING>
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = 'Silver de streaming dos Indicadores (append-only). Mesmas colunas da silver_indicadores_escola + metadados.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_inse_escola` (
  id_escola STRING,
  ano INT64,
  inse FLOAT64,
  quantidade_alunos_inse INT64,
  _message_id STRING,
  _publish_time TIMESTAMP,
  _ingestao_ts TIMESTAMP,
  _regras_aplicadas ARRAY<STRING>
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = 'Silver de streaming do INSE (append-only). Inclui quantidade_alunos_inse, usada na deduplicacao.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.silver_stream_censo_docente` (
  id_escola STRING,
  ano INT64,
  id_docente STRING,
  id_turma STRING,
  etapa_ensino STRING,
  idade INT64,
  escolaridade STRING,
  licenciatura_1 INT64,
  especializacao INT64,
  mestrado INT64,
  doutorado INT64,
  tipo_contratacao STRING,
  tipo_docente INT64,
  disciplina_matematica INT64,
  disciplina_lingua_portuguesa INT64,
  _message_id STRING,
  _publish_time TIMESTAMP,
  _ingestao_ts TIMESTAMP,
  _regras_aplicadas ARRAY<STRING>
)
PARTITION BY DATE(_ingestao_ts)
CLUSTER BY id_escola
OPTIONS (description = 'Silver de streaming dos docentes no grao docente x turma (append-only); a agregacao fica na view.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.auditoria_limpeza_stream` (
  message_id STRING,
  publish_time TIMESTAMP,
  processado_em TIMESTAMP,
  tabela STRING,
  ref STRING,
  id_escola STRING,
  status STRING,
  motivo STRING,
  regras_aplicadas ARRAY<STRING>,
  payload STRING
)
PARTITION BY DATE(processado_em)
CLUSTER BY tabela, status
OPTIONS (description = 'Uma linha por mensagem processada: destino (aceito/descartado/quarentena), regras aplicadas e, na quarentena, o payload cru.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.gold_ideb_features_stream` (
  id_escola STRING,
  ano INT64,
  ideb FLOAT64,
  sigla_uf STRING,
  id_municipio STRING,
  rede STRING,
  tipo_localizacao STRING,
  tipo_localizacao_diferenciada STRING,
  local_funcionamento_predio_escolar INT64,
  predio_compartilhado INT64,
  agua_potavel INT64,
  energia_rede_publica INT64,
  esgoto_rede_publica INT64,
  lixo_servico_coleta INT64,
  biblioteca INT64,
  laboratorio_ciencias INT64,
  laboratorio_informatica INT64,
  quadra_esportes INT64,
  patio_coberto INT64,
  refeitorio INT64,
  cozinha INT64,
  sala_multiuso INT64,
  banheiro INT64,
  banheiro_pne INT64,
  area_verde INT64,
  auditorio INT64,
  sala_atendimento_especial INT64,
  dependencia_pne INT64,
  acessibilidade_inexistente INT64,
  quantidade_sala_existente INT64,
  quantidade_sala_utilizada_climatizada INT64,
  equipamento_computador INT64,
  equipamento_lousa_digital INT64,
  equipamento_multimidia INT64,
  quantidade_computador_aluno INT64,
  quantidade_computador INT64,
  internet INT64,
  internet_alunos INT64,
  banda_larga INT64,
  quantidade_funcionario INT64,
  profissional_coordenador INT64,
  profissional_psicologo INT64,
  profissional_assistente_social INT64,
  profissional_bibliotecario INT64,
  alimentacao INT64,
  exame_selecao INT64,
  diurno INT64,
  noturno INT64,
  etapa_ensino_fundamental_anos_iniciais INT64,
  etapa_ensino_medio INT64,
  etapa_ensino_eja INT64,
  etapa_ensino_especial INT64,
  qtd_matricula_anos_finais INT64,
  qtd_matricula_fundamental INT64,
  qtd_matricula_feminino INT64,
  qtd_matricula_masculino INT64,
  qtd_matricula_branca INT64,
  qtd_matricula_preta INT64,
  qtd_matricula_parda INT64,
  qtd_matricula_indigena INT64,
  qtd_matricula_zona_rural INT64,
  qtd_matricula_transporte_publico INT64,
  qtd_docente_educacao_basica INT64,
  qtd_turma_anos_finais INT64,
  atu_ef_anos_finais FLOAT64,
  had_ef_anos_finais FLOAT64,
  afd_ef_anos_finais_grupo_1 FLOAT64,
  ird_media_regularidade_docente FLOAT64,
  ied_ef_anos_finais_nivel_1 FLOAT64,
  ied_ef_anos_finais_nivel_2 FLOAT64,
  ied_ef_anos_finais_nivel_3 FLOAT64,
  ied_ef_anos_finais_nivel_4 FLOAT64,
  ied_ef_anos_finais_nivel_5 FLOAT64,
  ied_ef_anos_finais_nivel_6 FLOAT64,
  icg_nivel_complexidade_gestao_escola STRING,
  inse FLOAT64,
  qtd_docente_anos_finais INT64,
  idade_media_docente FLOAT64,
  qtd_docente_escolaridade_superior INT64,
  qtd_docente_licenciatura INT64,
  qtd_docente_especializacao INT64,
  qtd_docente_mestrado INT64,
  qtd_docente_doutorado INT64,
  qtd_docente_efetivo INT64,
  qtd_docente_tipo_docente_regular INT64,
  qtd_docente_disciplina_matematica INT64,
  qtd_docente_disciplina_lingua_portuguesa INT64,
  origem STRING,
  primeira_carga_em TIMESTAMP,
  atualizado_em TIMESTAMP,
  versao INT64,
  _hash INT64
)
PARTITION BY DATE(primeira_carga_em)
CLUSTER BY sigla_uf, id_escola
OPTIONS (description = 'Gold de streaming: mesmas 87 colunas da gold_ideb_features, consolidada por MERGE em micro-lotes.');

CREATE TABLE IF NOT EXISTS `platinum-avenue-505512-t5.trabalho_pdm.predicoes_ideb_stream` (
  id_escola STRING,
  ano INT64,
  versao_gold INT64,
  ideb_real FLOAT64,
  ideb_previsto FLOAT64,
  erro_absoluto FLOAT64,
  modelo STRING,
  previsto_em TIMESTAMP
)
PARTITION BY DATE(previsto_em)
OPTIONS (description = 'Predicoes do modelo em producao para cada versao da gold de streaming.');
