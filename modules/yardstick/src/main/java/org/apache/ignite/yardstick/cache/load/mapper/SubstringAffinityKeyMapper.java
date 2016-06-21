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

package org.apache.ignite.yardstick.cache.load.mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;

/**
 * Affinity key mapper.
 */
public class SubstringAffinityKeyMapper implements AffinityKeyMapper {
    /**
     * Affinity key substring pattern.
     */
    private static Pattern AFFINITY_KEY_PATTERN = Pattern.compile("^clientId=([0-9a-zA-Z_]+).*$");

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {

        if (key instanceof String) {
            Matcher matcher = AFFINITY_KEY_PATTERN.matcher((CharSequence)key);
            if (matcher.find())
                return matcher.group(1);
        }

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {

    }
}
