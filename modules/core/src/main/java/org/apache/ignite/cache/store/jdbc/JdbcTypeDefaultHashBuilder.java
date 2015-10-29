package org.apache.ignite.cache.store.jdbc;

/**
 * Default implementation of {@link JdbcTypeHashBuilder}.
 *
 * This implementation ignores type and field names.
 */
public class JdbcTypeDefaultHashBuilder implements JdbcTypeHashBuilder {
    /** Hash code. */
    private int hash = 0;

    /** {@inheritDoc} */
    @Override public int toHash(Object val, String typeName, String fieldName) {
        hash = 31 * hash + (val != null ? val.hashCode() : 0);

        return hash;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return hash;
    }
}
