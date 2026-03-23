CREATE EXTERNAL TABLE IF NOT EXISTS bovespa_refined (
  date date,
  close double,
  high double,
  low double,
  open double,
  volume bigint,
  media_movel_3_dias double,
  data_processamento timestamp
)
STORED AS PARQUET
LOCATION 's3://tech-challenge-fase2-raquel-miranda/refined/';