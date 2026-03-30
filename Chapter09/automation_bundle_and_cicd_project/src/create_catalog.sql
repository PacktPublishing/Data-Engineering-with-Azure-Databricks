DECLARE env STRING;
SET VAR env = '<env>'; --dev/stagin

CREATE SCHEMA IDENTIFIER('analytics_' || env);
