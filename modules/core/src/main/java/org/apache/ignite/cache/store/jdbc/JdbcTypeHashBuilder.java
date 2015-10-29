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

/**
 * API for implementing custom hashing logic for portable objects on server side.
 */
public interface JdbcTypeHashBuilder {
    /**
     * Calculate hash code for specified value and field name.
     *
     * @param val Value to calculate hash code for.
     * @param typeName Type name hash is calculating for.
     * @param fieldName Field name that should participate in hash code calculation.
     * @return Hash code.
     */
    public int toHash(Object val, String typeName, String fieldName);

    /**
     * @return Calculated hash code.
     */
    public int hash();
}
