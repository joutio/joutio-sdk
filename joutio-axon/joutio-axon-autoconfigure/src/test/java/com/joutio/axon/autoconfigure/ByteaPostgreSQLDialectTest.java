package com.joutio.axon.autoconfigure;

import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteaPostgreSQLDialectTest {

    private final ByteaPostgreSQLDialect dialect = new ByteaPostgreSQLDialect();

    @Test
    void blobColumnsAreBytea() {
        assertThat(dialect.columnType(SqlTypes.BLOB)).isEqualTo("bytea");
        assertThat(dialect.castType(SqlTypes.BLOB)).isEqualTo("bytea");
    }

    @Test
    void otherColumnsKeepPostgresTypes() {
        assertThat(dialect.columnType(SqlTypes.VARCHAR)).startsWith("varchar");
        assertThat(dialect.columnType(SqlTypes.TIMESTAMP_UTC)).startsWith("timestamp");
    }
}
