/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.spark.obkv;

import java.io.Serializable;


import com.alipay.oceanbase.rpc.table.api.TableQuery;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * An {@link InputPartition} implementation for OBKV that represents a scan range.
 *
 * <p>For non-partitioned tables, a single full-scan partition is used. For partitioned tables, each
 * OceanBase partition maps to one Spark input partition with specific scan range boundaries.
 */
public class OBKVPartition implements InputPartition, Serializable {

    private static final long serialVersionUID = 1L;

    private final int partitionIndex;
    private final Object[] scanRangeStart;
    private final boolean startInclusive;
    private final Object[] scanRangeEnd;
    private final boolean endInclusive;

    private OBKVPartition(
            int partitionIndex,
            Object[] scanRangeStart,
            boolean startInclusive,
            Object[] scanRangeEnd,
            boolean endInclusive) {
        this.partitionIndex = partitionIndex;
        this.scanRangeStart = scanRangeStart;
        this.startInclusive = startInclusive;
        this.scanRangeEnd = scanRangeEnd;
        this.endInclusive = endInclusive;
    }

    /** Creates a single full-scan partition for non-partitioned tables. */
    public static OBKVPartition fullScan() {
        return new OBKVPartition(0, null, true, null, true);
    }

    /** Creates a partition with specific scan range boundaries. */
    public static OBKVPartition ofRange(
            int partitionIndex,
            Object[] start,
            boolean startInclusive,
            Object[] end,
            boolean endInclusive) {
        return new OBKVPartition(partitionIndex, start, startInclusive, end, endInclusive);
    }

    /**
     * Applies the scan range of this partition to a {@link TableQuery}.
     *
     * @param query the OBKV table query to apply scan range to
     */
    public void applyScanRange(TableQuery query) {
        if (scanRangeStart != null || scanRangeEnd != null) {
            Object[] start = scanRangeStart != null ? scanRangeStart : new Object[] {};
            Object[] end = scanRangeEnd != null ? scanRangeEnd : new Object[] {};
            query.addScanRange(start, startInclusive, end, endInclusive);
        }
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }
}
