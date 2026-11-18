package com.joutio.axon.autoconfigure;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.BinaryJdbcType;

import java.sql.Types;

/**
 * Dialecte Postgres du SDK : les {@code @Lob byte[]} (entités Axon : tokens, sagas, événements,
 * DLQ) sont stockés en {@code bytea}, pas en large objects ({@code oid}).
 *
 * <p>Avec {@code oid}, chaque écriture crée un large object et la mise à jour d'une ligne n'efface
 * jamais l'ancien : les tokens des processors, mis à jour à chaque événement, laissaient des
 * dizaines de milliers d'orphelins par jour dans {@code pg_largeobject}. Le schéma
 * ({@code V0__init_axon_schema.sql}) déclare ces colonnes en {@code bytea}.</p>
 *
 * <p>Posé par défaut ({@code spring.jpa.properties.hibernate.dialect}) par
 * {@link AxonDistributedEnvironmentPostProcessor}.</p>
 */
public class ByteaPostgreSQLDialect extends PostgreSQLDialect {

    @Override
    protected String columnType(int sqlTypeCode) {
        return sqlTypeCode == SqlTypes.BLOB ? "bytea" : super.columnType(sqlTypeCode);
    }

    @Override
    protected String castType(int sqlTypeCode) {
        return sqlTypeCode == SqlTypes.BLOB ? "bytea" : super.castType(sqlTypeCode);
    }

    @Override
    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        typeContributions.getTypeConfiguration().getJdbcTypeRegistry()
                .addDescriptor(Types.BLOB, BinaryJdbcType.INSTANCE);
    }
}
