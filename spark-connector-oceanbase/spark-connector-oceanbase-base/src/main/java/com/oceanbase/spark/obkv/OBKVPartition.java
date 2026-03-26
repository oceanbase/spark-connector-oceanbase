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


import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * An {@link InputPartition} implementation for OBKV that represents a scan range.
 *
 * <p>For non-partitioned tables, a single full-scan partition is used. For partitioned tables, each
 * OceanBase partition maps to one Spark input partition with specific partition information.
 */
public class OBKVPartition implements InputPartition, Serializable {

    private static final long serialVersionUID = 1L;

    private final int partitionIndex;
    private final Object[] scanRangeStart;
    private final boolean startInclusive;
    private final Object[] scanRangeEnd;
    private final boolean endInclusive;

    // Partition information for parallel reading
    private final long partitionId; // partition id in 3.x aka tablet id in 4.x
    private final long partId; // logic id
    private final long tableId;
    private final String ip;
    private final int port;
    private final long lsId;

    private OBKVPartition(
            int partitionIndex,
            Object[] scanRangeStart,
            boolean startInclusive,
            Object[] scanRangeEnd,
            boolean endInclusive,
            long partitionId,
            long partId,
            long tableId,
            String ip,
            int port,
            long lsId) {
        this.partitionIndex = partitionIndex;
        this.scanRangeStart = scanRangeStart;
        this.startInclusive = startInclusive;
        this.scanRangeEnd = scanRangeEnd;
        this.endInclusive = endInclusive;
        this.partitionId = partitionId;
        this.partId = partId;
        this.tableId = tableId;
        this.ip = ip;
        this.port = port;
        this.lsId = lsId;
    }

    /** Creates a single full-scan partition for non-partitioned tables. */
    public static OBKVPartition fullScan() {
        return new OBKVPartition(
                0,
                null,
                true,
                null,
                true,
                Constants.OB_INVALID_ID,
                Constants.OB_INVALID_ID,
                Constants.OB_INVALID_ID,
                "",
                -1,
                Constants.INVALID_LS_ID);
    }

    /** Creates a partition with specific scan range boundaries. */
    public static OBKVPartition ofRange(
            int partitionIndex,
            Object[] start,
            boolean startInclusive,
            Object[] end,
            boolean endInclusive) {
        return new OBKVPartition(
                partitionIndex,
                start,
                startInclusive,
                end,
                endInclusive,
                Constants.OB_INVALID_ID,
                Constants.OB_INVALID_ID,
                Constants.OB_INVALID_ID,
                "",
                -1,
                Constants.INVALID_LS_ID);
    }

    /**
     * Creates a partition with OBKV partition information for parallel reading.
     *
     * @param partitionIndex the Spark partition index
     * @param partitionId the OBKV partition id (tablet id in 4.x)
     * @param partId the OBKV logical partition id
     * @param tableId the table id
     * @param ip the server ip
     * @param port the server port
     * @param lsId the log stream id
     * @return a new OBKVPartition instance
     */
    public static OBKVPartition ofPartition(
            int partitionIndex,
            long partitionId,
            long partId,
            long tableId,
            String ip,
            int port,
            long lsId) {
        return new OBKVPartition(
                partitionIndex,
                null,
                true,
                null,
                true,
                partitionId,
                partId,
                tableId,
                ip,
                port,
                lsId);
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
        // For partition-aware reading, the OBKV client will automatically route
        // to the correct partition based on the partitionId/tabletId
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getPartId() {
        return partId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public long getLsId() {
        return lsId;
    }

    /**
     * Checks if this partition has valid partition information.
     *
     * @return true if this is a partition-aware scan
     */
    public boolean hasPartitionInfo() {
        return partitionId != Constants.OB_INVALID_ID;
    }

    @Override
    public String toString() {
        return "OBKVPartition{"
                + "partitionIndex="
                + partitionIndex
                + ", partitionId="
                + partitionId
                + ", partId="
                + partId
                + ", tableId="
                + tableId
                + ", lsId="
                + lsId
                + ", ip='"
                + ip
                + '\''
                + ", port="
                + port
                + '}';
    }
}
