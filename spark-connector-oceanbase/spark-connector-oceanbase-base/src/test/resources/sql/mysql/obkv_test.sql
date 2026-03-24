-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE IF NOT EXISTS obkv_products (
  id          INTEGER      NOT NULL,
  name        VARCHAR(255) NOT NULL DEFAULT 'default',
  description VARCHAR(512),
  weight      DOUBLE,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS obkv_composite_pk (
  tenant_id   INTEGER      NOT NULL,
  product_id  INTEGER      NOT NULL,
  name        VARCHAR(255),
  price       DOUBLE,
  PRIMARY KEY(tenant_id, product_id)
);

CREATE TABLE IF NOT EXISTS obkv_all_types (
  pk_id       INTEGER       NOT NULL PRIMARY KEY,
  col_bool    BOOLEAN,
  col_tinyint TINYINT,
  col_small   SMALLINT,
  col_int     INT,
  col_bigint  BIGINT,
  col_float   FLOAT,
  col_double  DOUBLE,
  col_decimal DOUBLE,
  col_varchar VARCHAR(255),
  col_date    DATE,
  col_ts      TIMESTAMP    NULL DEFAULT NULL
);

-- Partitioned table for testing parallel reading
CREATE TABLE IF NOT EXISTS obkv_partitioned (
  id          BIGINT       NOT NULL,
  name        VARCHAR(255) NOT NULL,
  value       DOUBLE,
  PRIMARY KEY(id)
) PARTITION BY RANGE(id) (
  PARTITION p0 VALUES LESS THAN (1000),
  PARTITION p1 VALUES LESS THAN (2000),
  PARTITION p2 VALUES LESS THAN (3000),
  PARTITION p3 VALUES LESS THAN MAXVALUE
);
