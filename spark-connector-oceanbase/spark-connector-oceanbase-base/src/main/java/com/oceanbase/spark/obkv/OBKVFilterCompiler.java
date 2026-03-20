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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.filter.ObTableFilterList;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;

/**
 * Compiles Spark {@link Filter} instances into OBKV server-side filters ({@link ObTableFilter}).
 *
 * <p>Filters on non-primary-key columns are converted to {@link ObTableValueFilter} for server-side
 * evaluation. Filters that cannot be pushed down are returned as unhandled filters for Spark-side
 * evaluation.
 */
public class OBKVFilterCompiler {

    private final Set<String> primaryKeyColumns;

    public OBKVFilterCompiler(String[] primaryKeys) {
        this.primaryKeyColumns = new HashSet<>(Arrays.asList(primaryKeys));
    }

    /** The result of compiling Spark filters. */
    public static class CompileResult {
        private final ObTableFilter serverFilter;
        private final Filter[] unhandledFilters;

        public CompileResult(ObTableFilter serverFilter, Filter[] unhandledFilters) {
            this.serverFilter = serverFilter;
            this.unhandledFilters = unhandledFilters;
        }

        public ObTableFilter getServerFilter() {
            return serverFilter;
        }

        public Filter[] getUnhandledFilters() {
            return unhandledFilters;
        }
    }

    /**
     * Compiles an array of Spark Filters into OBKV server-side filters.
     *
     * @param filters Spark filters to compile
     * @return compilation result with server filter and unhandled filters
     */
    public CompileResult compile(Filter[] filters) {
        List<ObTableFilter> compiledFilters = new ArrayList<>();
        List<Filter> unhandled = new ArrayList<>();

        for (Filter filter : filters) {
            ObTableFilter compiled = compileFilter(filter);
            if (compiled != null) {
                compiledFilters.add(compiled);
            } else {
                unhandled.add(filter);
            }
        }

        ObTableFilter serverFilter = null;
        if (compiledFilters.size() == 1) {
            serverFilter = compiledFilters.get(0);
        } else if (compiledFilters.size() > 1) {
            serverFilter =
                    new ObTableFilterList(
                            ObTableFilterList.operator.AND,
                            compiledFilters.toArray(new ObTableFilter[0]));
        }

        return new CompileResult(serverFilter, unhandled.toArray(new Filter[0]));
    }

    private ObTableFilter compileFilter(Filter filter) {
        if (filter instanceof EqualTo) {
            EqualTo f = (EqualTo) filter;
            return new ObTableValueFilter(ObCompareOp.EQ, f.attribute(), f.value());
        } else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            return new ObTableValueFilter(ObCompareOp.GT, f.attribute(), f.value());
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            return new ObTableValueFilter(ObCompareOp.GE, f.attribute(), f.value());
        } else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            return new ObTableValueFilter(ObCompareOp.LT, f.attribute(), f.value());
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            return new ObTableValueFilter(ObCompareOp.LE, f.attribute(), f.value());
        } else if (filter instanceof IsNull) {
            IsNull f = (IsNull) filter;
            return new ObTableValueFilter(ObCompareOp.IS, f.attribute(), null);
        } else if (filter instanceof IsNotNull) {
            IsNotNull f = (IsNotNull) filter;
            return new ObTableValueFilter(ObCompareOp.IS_NOT, f.attribute(), null);
        } else if (filter instanceof And) {
            And f = (And) filter;
            ObTableFilter left = compileFilter(f.left());
            ObTableFilter right = compileFilter(f.right());
            if (left != null && right != null) {
                return new ObTableFilterList(ObTableFilterList.operator.AND, left, right);
            }
            return null;
        } else if (filter instanceof Or) {
            Or f = (Or) filter;
            ObTableFilter left = compileFilter(f.left());
            ObTableFilter right = compileFilter(f.right());
            if (left != null && right != null) {
                return new ObTableFilterList(ObTableFilterList.operator.OR, left, right);
            }
            return null;
        } else if (filter instanceof Not) {
            // OBKV does not natively support NOT, skip it
            return null;
        }
        return null;
    }

    /**
     * Checks if a filter references a primary key column.
     *
     * @param filter the filter to check
     * @return true if the filter is on a primary key column
     */
    public boolean isPrimaryKeyFilter(Filter filter) {
        if (filter instanceof EqualTo) {
            return primaryKeyColumns.contains(((EqualTo) filter).attribute());
        } else if (filter instanceof GreaterThan) {
            return primaryKeyColumns.contains(((GreaterThan) filter).attribute());
        } else if (filter instanceof GreaterThanOrEqual) {
            return primaryKeyColumns.contains(((GreaterThanOrEqual) filter).attribute());
        } else if (filter instanceof LessThan) {
            return primaryKeyColumns.contains(((LessThan) filter).attribute());
        } else if (filter instanceof LessThanOrEqual) {
            return primaryKeyColumns.contains(((LessThanOrEqual) filter).attribute());
        }
        return false;
    }
}
