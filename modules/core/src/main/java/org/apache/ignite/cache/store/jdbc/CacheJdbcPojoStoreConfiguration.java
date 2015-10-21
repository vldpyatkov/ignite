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

import java.io.Serializable;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;

/**
 * JDBC POJO store configuration.
 */
public class CacheJdbcPojoStoreConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value for write attempts. */
    protected static final int DFLT_WRITE_ATTEMPTS = 2;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Types that store could process. */
    private CacheJdbcPojoStoreType[] types;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Database dialect. */
    protected JdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSz = Runtime.getRuntime().availableProcessors();

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int maxWrtAttempts = DFLT_WRITE_ATTEMPTS;

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSz = DFLT_BATCH_SIZE;

    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;
}
