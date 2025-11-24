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

CREATE TABLE products
(
  id          INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight      DECIMAL(20, 10)
);

CREATE TABLE products_no_pri_key
(
  id          INTEGER      NOT NULL ,
  name        VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight      DECIMAL(20, 10)
) partition by key(id)
(partition `p0`,
 partition `p1`,
 partition `p2`);

CREATE TABLE products_full_pri_key
(
  id          INTEGER      NOT NULL ,
  name        VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight      DECIMAL(20, 10),
  primary key(id, name, description, weight)
);

CREATE TABLE products_no_int_pri_key
(
  id          VARCHAR(255)      NOT NULL ,
  name        VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight      DECIMAL(20, 10),
  primary key(id, name)
);

CREATE TABLE products_unique_key
(
  id          INTEGER ,
  name        VARCHAR(255),
  description VARCHAR(512),
  weight      DECIMAL(20, 10),
  unique index unique_idx(id, name)
) partition by key(id)
(partition `p0`,
 partition `p1`,
 partition `p2`);

CREATE TABLE products_full_unique_key
(
  id          INTEGER ,
  name        VARCHAR(255) ,
  description VARCHAR(512),
  weight      DECIMAL(20, 10),
  unique index unique_idx(id, name, description, weight)
);

-- Tables for upsert-by-unique-key behavior tests
CREATE TABLE products_pri_and_unique_key
(
  id          INTEGER      NOT NULL,
  name        VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight      DECIMAL(20, 10),
  PRIMARY KEY(id, name),
  UNIQUE INDEX unique_idx(name)
);

CREATE TABLE products_with_decimal
(
  id          DECIMAL(30, 0),
  weight      DECIMAL(20, 10)
);
