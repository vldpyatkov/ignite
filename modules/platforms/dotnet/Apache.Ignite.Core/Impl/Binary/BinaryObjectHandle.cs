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

namespace Apache.Ignite.Core.Impl.Binary
{
    /// <summary>
    /// Object handle. Wraps a single value.
    /// </summary>
    internal class BinaryObjectHandle
    {
        /** Value. */
        private readonly object _val;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryObjectHandle"/> class.
        /// </summary>
        /// <param name="val">The value.</param>
        public BinaryObjectHandle(object val)
        {
            _val = val;
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public object Value
        {
            get { return _val; }
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            var that = obj as BinaryObjectHandle;

            return that != null && _val == that._val;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return _val != null ? _val.GetHashCode() : 0;
        }
    }
}
