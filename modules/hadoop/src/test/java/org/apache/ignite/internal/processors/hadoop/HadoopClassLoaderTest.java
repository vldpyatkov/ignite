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

package org.apache.ignite.internal.processors.hadoop;

import javax.security.auth.AuthPermission;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.internal.processors.hadoop.deps.CircularDependencyHadoop;
import org.apache.ignite.internal.processors.hadoop.deps.CircularDependencyNoHadoop;
import org.apache.ignite.internal.processors.hadoop.deps.DependencyNoHadoop;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopCasting;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopClassAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopConstructorInvocation;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopDeclaredCheckedExceptionInMethod;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopDeclaredRuntimeExceptionInMethod;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopExtends;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopField;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopImplements;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopInitializer;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopInnerClass;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopLocalVariableType;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopMethodAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopMethodInvocation;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopMethodParameter;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopMethodReturnType;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopOuterClass;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopParameterAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopStaticField;
import org.apache.ignite.internal.processors.hadoop.deps.HadoopStaticInitializer;
import org.apache.ignite.internal.processors.hadoop.deps.NoHadoop;

/**
 *
 */
public class HadoopClassLoaderTest extends TestCase {
    /** */
    final HadoopClassLoader ldr = new HadoopClassLoader(null, "test");

    /**
     * @throws Exception If failed.
     */
    public void testClassLoading() throws Exception {
        assertNotSame(CircularDependencyHadoop.class, ldr.loadClass(CircularDependencyHadoop.class.getName()));
        assertNotSame(CircularDependencyNoHadoop.class, ldr.loadClass(CircularDependencyNoHadoop.class.getName()));

        assertSame(NoHadoop.class, ldr.loadClass(NoHadoop.class.getName()));
    }

    /**
     *
     */
    public void testDependencySearch() {
        // Various positive cases of Hadoop classes dependency:
        final Class[] positiveClasses = {
            // Hadoop class itself:
            Configuration.class,
            // Class for that org.apache.ignite.internal.processors.hadoop.HadoopClassLoader.isHadoopIgfs returns true:
            HadoopUtils.class,

            HadoopStaticField.class,
            HadoopCasting.class,
            HadoopClassAnnotation.class,
            HadoopConstructorInvocation.class,
            HadoopDeclaredCheckedExceptionInMethod.class,
            HadoopDeclaredRuntimeExceptionInMethod.class,
            HadoopExtends.class,
            HadoopField.class,
            HadoopImplements.class,
            HadoopInitializer.class,

            // TODO: actually the 2 below classes do not depend on Hadoop, should not be detected as such.
            // TODO: but for now they are, so this behavior is asserted in test:
            HadoopInnerClass.class,
            HadoopOuterClass.InnerNoHadoop.class,

            HadoopLocalVariableType.class,
            HadoopMethodAnnotation.class,
            HadoopMethodInvocation.class,
            HadoopMethodParameter.class,
            HadoopMethodReturnType.class,
            HadoopParameterAnnotation.class,
            HadoopStaticField.class,
            HadoopStaticInitializer.class,

            DependencyNoHadoop.class,
            CircularDependencyHadoop.class,
            CircularDependencyNoHadoop.class,
        };

        for (Class c: positiveClasses)
            assertTrue(c.getName(), ldr.hasExternalDependencies(c.getName()));

        // Negative cases:
        final Class[] negativeClasses = {
            // java.lang.*:
            Object.class,
            // javax.*:
            AuthPermission.class,
            NoHadoop.class,
        };

        for (Class c: negativeClasses)
            assertFalse(c.getName(),
                ldr.hasExternalDependencies(c.getName()));
    }
}