/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.cache.store.jdbc.dialect.*;

import java.io.*;

/**
 * JDBC POJO store configuration.
 */
public class CacheJdbcPojoStoreConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value for write attempts. */
    public static final int DFLT_WRITE_ATTEMPTS = 2;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSz = DFLT_BATCH_SIZE;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Database dialect. */
    protected JdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSz = Runtime.getRuntime().availableProcessors();

    /** Maximum write attempts in case of database error. */
    private int maxWrtAttempts = DFLT_WRITE_ATTEMPTS;

    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;

    /** Types that store could process. */
    private CacheJdbcPojoStoreType[] types;

    /**
     * Empty constructor (all values are initialized to their defaults).
     */
    public CacheJdbcPojoStoreConfiguration() {
        /* No-op. */
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public CacheJdbcPojoStoreConfiguration(CacheJdbcPojoStoreConfiguration cfg) {
        // Order alphabetically for maintenance purposes.
        batchSz = cfg.getBatchSize();
        dataSrcBean = cfg.getDataSourceBean();
        dialect = cfg.getDialect();
        maxPoolSz = cfg.getMaximumPoolSize();
        maxWrtAttempts = cfg.getMaximumWriteAttempts();
        parallelLoadCacheMinThreshold = cfg.getParallelLoadCacheMinimumThreshold();
        types = cfg.getTypes();
    }

    /**
     * Get maximum batch size for delete and delete operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSz;
    }

    /**
     * Set maximum batch size for write and delete operations.
     *
     * @param batchSz Maximum batch size.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setBatchSize(int batchSz) {
        this.batchSz = batchSz;

        return this;
    }

    /**
     * Gets name of the data source bean.
     *
     * @return Data source bean name.
     */
    public String getDataSourceBean() {
        return dataSrcBean;
    }

    /**
     * Sets name of the data source bean.
     *
     * @param dataSrcBean Data source bean name.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setDataSourceBean(String dataSrcBean) {
        this.dataSrcBean = dataSrcBean;

        return this;
    }

    /**
     * Get database dialect.
     *
     * @return Database dialect.
     */
    public JdbcDialect getDialect() {
        return dialect;
    }

    /**
     * Set database dialect.
     *
     * @param dialect Database dialect.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setDialect(JdbcDialect dialect) {
        this.dialect = dialect;

        return this;
    }

    /**
     * Get maximum workers thread count. These threads are responsible for queries execution.
     *
     * @return Maximum workers thread count.
     */
    public int getMaximumPoolSize() {
        return maxPoolSz;
    }

    /**
     * Set Maximum workers thread count. These threads are responsible for queries execution.
     *
     * @param maxPoolSz Max workers thread count.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setMaximumPoolSize(int maxPoolSz) {
        this.maxPoolSz = maxPoolSz;

        return this;
    }

    /**
     * Gets maximum number of write attempts in case of database error.
     *
     * @return Maximum number of write attempts.
     */
    public int getMaximumWriteAttempts() {
        return maxWrtAttempts;
    }

    /**
     * Sets maximum number of write attempts in case of database error.
     *
     * @param maxWrtAttempts Number of write attempts.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setMaximumWriteAttempts(int maxWrtAttempts) {
        this.maxWrtAttempts = maxWrtAttempts;

        return this;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @return If {@code 0} then load sequentially.
     */
    public int getParallelLoadCacheMinimumThreshold() {
        return parallelLoadCacheMinThreshold;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @param parallelLoadCacheMinThreshold Minimum row count threshold. If {@code 0} then load sequentially.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setParallelLoadCacheMinimumThreshold(int parallelLoadCacheMinThreshold) {
        this.parallelLoadCacheMinThreshold = parallelLoadCacheMinThreshold;

        return this;
    }

    /**
     * Gets types known by store.
     *
     * @return Types known by store.
     */
    public CacheJdbcPojoStoreType[] getTypes() {
        return types;
    }

    /**
     * Sets store configurations.
     *
     * @param types Store should process.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreConfiguration setTypes(CacheJdbcPojoStoreType... types) {
        this.types = types;

        return this;
    }
}
