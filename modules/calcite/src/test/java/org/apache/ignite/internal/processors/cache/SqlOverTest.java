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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SqlOverTest extends GridCommonAbstractTest {

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()))
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setSqlSchema("PUBLIC")
                .setSqlFunctionClasses(SessionContextSqlFunctionTest.SessionContextSqlFunctions.class));
    }

    private static String COUNT_OVER_EX1 = "SELECT \n" +
        "  emp_id,\n" +
        "  name,\n" +
        "  department_id,\n" +
        "  salary,\n" +
        "  COUNT(*) OVER() as total_employees\n" +
        "FROM employee\n" +
        "ORDER BY emp_id;";

    /** Node. */
    private IgniteEx node;

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        node = startGrid(0);
    }

    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testEmployeeTableCount() {
        // Create employee table
        queryAndPrint(node, "CREATE TABLE IF NOT EXISTS employee ("
            + "emp_id INTEGER PRIMARY KEY, "
            + "name VARCHAR, "
            + "department_id INTEGER,"
            + "manager_id INTEGER,"
            + "salary DECIMAL(10,2))");

        // Insert test data
        queryAndPrint(node, "INSERT INTO employee VALUES "
            + "(1, 'John Doe', 1, NULL, 100000), "
            + "(2, 'Jane Smith', 2, 1, 80000), "
            + "(3, 'Bob Johnson', 3, 1, 95000), "
            + "(4, 'Alice Brown', 2, 2, 55000), "
            + "(5, 'Charlie Wilson', 2, 2, 60000)");

        // Test SELECT all employees
        List<List<?>> result = queryAndPrint(node, COUNT_OVER_EX1);

        assertEquals(5, result.size());
    }

    /** */
    private List<List<?>> queryAndPrint(Ignite ign, String sql, Object... args) {
        List<List<?>> result = ign.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        printResult(sql, result);

        return result;
    }

    /**
     * Prints query results in readable format.
     *
     * @param sql SQL query.
     * @param result Query result.
     */
    private void printResult(String sql, List<List<?>> result) {
        info("Executing query: " + sql);

        if (sql.trim().toUpperCase().startsWith("INSERT") ||
            sql.trim().toUpperCase().startsWith("UPDATE") ||
            sql.trim().toUpperCase().startsWith("DELETE") ||
            sql.trim().toUpperCase().startsWith("CREATE") ||
            sql.trim().toUpperCase().startsWith("DROP")) {
            info("Command executed successfully");
            if (!result.isEmpty()) {
                info("Rows affected: " + result.get(0).get(0));
            }
            return;
        }

        if (result.isEmpty()) {
            info("No results");
            return;
        }

        // Print header
        StringBuilder header = new StringBuilder();
        header.append("\nResults (").append(result.size()).append(" rows):\n");

        // Create separator
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            separator.append("-");
        }

        info(header.toString());
        info(separator.toString());

        // Print rows
        int rowNum = 1;
        for (List<?> row : result) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row ").append(rowNum++).append(": ");

            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    rowBuilder.append(" | ");
                }

                Object value = row.get(i);
                if (value == null) {
                    rowBuilder.append("NULL");
                } else {
                    // Format the value based on type
                    if (value instanceof String) {
                        rowBuilder.append("'").append(value).append("'");
                    } else if (value instanceof Long || value instanceof Integer) {
                        rowBuilder.append(value);
                    } else {
                        rowBuilder.append(value.toString());
                    }
                }
            }

            info(rowBuilder.toString());
        }

        info(separator.toString());
    }

}
