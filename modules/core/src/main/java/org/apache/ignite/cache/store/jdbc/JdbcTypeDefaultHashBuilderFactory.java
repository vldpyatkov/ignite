package org.apache.ignite.cache.store.jdbc;

import javax.cache.configuration.Factory;

/**
 * Factory for creating default hash builder.
 */
public class JdbcTypeDefaultHashBuilderFactory implements Factory<JdbcTypeHashBuilder> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Instance of factory for reuse. */
    public final static JdbcTypeDefaultHashBuilderFactory INSTANCE = new JdbcTypeDefaultHashBuilderFactory();

    /** {@inheritDoc} */
    @Override public JdbcTypeHashBuilder create() {
        return new JdbcTypeDefaultHashBuilder();
    }
}
