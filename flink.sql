INSERT INTO `anomalies-topic`
SELECT 
  CAST(JSON_VALUE(CAST(`val` AS STRING), '$.userid') AS BYTES) AS key,  -- or use 'anomaly'
  CAST(
    CONCAT(
      '{"userid":"', JSON_VALUE(CAST(`val` AS STRING), '$.userid'),
      '","symbol":"', JSON_VALUE(CAST(`val` AS STRING), '$.symbol'),
      '","quantity":', JSON_VALUE(CAST(`val` AS STRING), '$.quantity'),
      '","side":"', JSON_VALUE(CAST(`val` AS STRING), '$.side'),
      '","price":', JSON_VALUE(CAST(`val` AS STRING), '$.price'),
      '","account":"', JSON_VALUE(CAST(`val` AS STRING), '$.account'),
      '","alert_time":"', CAST(CURRENT_TIMESTAMP AS STRING), '"}'
    ) AS BYTES
  ) AS val
FROM `datagen-topic`
WHERE CAST(JSON_VALUE(CAST(`val` AS STRING), '$.quantity') AS INT) > 3000;




SELECT 
  JSON_VALUE(CAST(`val` AS STRING), '$.userid') AS userid,
  JSON_VALUE(CAST(`val` AS STRING), '$.symbol') AS symbol,
  CAST(JSON_VALUE(CAST(`val` AS STRING), '$.quantity') AS INT) AS quantity,
  CURRENT_TIMESTAMP AS alert_time
FROM `datagen-topic`
WHERE CAST(JSON_VALUE(CAST(`val` AS STRING), '$.quantity') AS INT) > 3000;

SELECT 
  JSON_VALUE(CAST(`val` AS STRING), '$.userid') AS userid,
  JSON_VALUE(CAST(`val` AS STRING), '$.symbol') AS symbol,
  CAST(JSON_VALUE(CAST(`val` AS STRING), '$.price') AS DOUBLE) AS price
FROM `datagen-topic`
WHERE CAST(JSON_VALUE(CAST(`val` AS STRING), '$.price') AS DOUBLE) > 800;

INSERT INTO `fraud-topic`
SELECT 
  CAST(JSON_VALUE(CAST(`val` AS STRING), '$.userid') AS BYTES) AS key,  -- or use 'anomaly'
  CAST(
    CONCAT(
      '{"userid":"', JSON_VALUE(CAST(`val` AS STRING), '$.userid'),
      '","symbol":"', JSON_VALUE(CAST(`val` AS STRING), '$.symbol'),
      '","quantity":', JSON_VALUE(CAST(`val` AS STRING), '$.quantity'),
      '","side":"', JSON_VALUE(CAST(`val` AS STRING), '$.side'),
      '","price":', JSON_VALUE(CAST(`val` AS STRING), '$.price'),
      '","account":"', JSON_VALUE(CAST(`val` AS STRING), '$.account'),
      '","alert_time":"', CAST(CURRENT_TIMESTAMP AS STRING), '"}'
    ) AS BYTES
  ) AS val
FROM `datagen-topic`
WHERE CAST(JSON_VALUE(CAST(`val` AS STRING), '$.price') AS INT) > 800;

