package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.cache.IgniteObject;

import java.util.Collection;

/**
 * Default implementation of {@link JdbcTypeHasher}.
 *
 * This implementation ignores type and field names.
 */
public class JdbcTypeDefaultHasher implements JdbcTypeHasher {
    /** */
    private static final long serialVersionUID = 0L;

    /** Singleton instance to use. */
    public static final JdbcTypeHasher INSTANCE = new JdbcTypeDefaultHasher();

    /** {@inheritDoc} */
    @Override public int hashCode(IgniteObject obj, Collection<String> fields) {
        int hash = 0;

        for (String field : fields) {
            Object val = obj.field(field);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
    }
}
