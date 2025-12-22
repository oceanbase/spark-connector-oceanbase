-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create products table for Oracle mode testing
CREATE TABLE products (
    id NUMBER(10) NOT NULL,
    name VARCHAR2(255),
    description VARCHAR2(1000),
    weight NUMBER(10,2),
    CONSTRAINT pk_products PRIMARY KEY (id)
);

CREATE TABLE products_no_pri_key (
    id NUMBER(10) NOT NULL,
    name VARCHAR2(255) NOT NULL,
    description VARCHAR2(1000),
    weight NUMBER(10,2)
) PARTITION BY HASH(id) PARTITIONS 3;

CREATE TABLE products_full_pri_key (
    id NUMBER(10) NOT NULL,
    name VARCHAR2(255) NOT NULL,
    description VARCHAR2(1000),
    weight NUMBER(10,2),
    CONSTRAINT pk_products_full PRIMARY KEY (id, name, description, weight)
);

CREATE TABLE products_no_int_pri_key (
    id VARCHAR2(255) NOT NULL,
    name VARCHAR2(255) NOT NULL,
    description VARCHAR2(1000),
    weight NUMBER(10,2),
    CONSTRAINT pk_products_no_int PRIMARY KEY (id, name)
);

CREATE TABLE products_unique_key (
    id NUMBER(10),
    name VARCHAR2(255),
    description VARCHAR2(1000),
    weight NUMBER(10,2),
    CONSTRAINT uk_products_unique UNIQUE (id, name)
) PARTITION BY HASH(id) PARTITIONS 3;

CREATE TABLE products_full_unique_key (
    id NUMBER(10),
    name VARCHAR2(255),
    description VARCHAR2(1000),
    weight NUMBER(10,2),
    CONSTRAINT uk_products_full_unique UNIQUE (id, name, description, weight)
);


