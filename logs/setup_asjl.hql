CREATE DATABASE IF NOT EXISTS ASJL;
USE ASJL;

DROP TABLE IF EXISTS pm_csv_ext;
DROP TABLE IF EXISTS pm_orc;

CREATE EXTERNAL TABLE pm_csv_ext (
  deviceid INT,
  devicename INT,
  resourcename STRING,
  collectiontime STRING,
  granularityperiod INT,
  rxunicast STRING,
  rxmulcast STRING,
  rxbrdcast STRING,
  txunicast STRING,
  txmulcast STRING,
  txbrdcast STRING,
  rxfullbgood STRING,
  txfullbgood STRING,
  txpkts STRING,
  rxpkts STRING,
  port_rx_bw_utilization STRING,
  port_tx_bw_utilization STRING,
  eth_rx_throughput_max STRING,
  eth_rx_throughput_min STRING,
  eth_rx_throughput_avg STRING,
  rx_drop_ratio DOUBLE,
  tx_drop_ratio DOUBLE,
  eth_tx_throughput_max STRING,
  eth_tx_throughput_min STRING,
  eth_tx_throughput_avg STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/HadoopLab/AndresSanchez/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE TABLE pm_orc AS SELECT * FROM pm_csv_ext;
DESCRIBE FORMATTED pm_orc;
