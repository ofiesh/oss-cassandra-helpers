package com.clearcapital.oss.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.ClassRule;
import org.junit.Test;

import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CQLHelpersTest {

    @ClassRule
    public static CassandraTestResource cassandraResource = new CassandraTestResource(Arrays.asList("cassandra.yaml"));

    @Test
    public void testEscapeCqlString() {
        assertEquals("A fine \'\'mess\'\' you\'\'ve gotten us into, Ollie.",
                CQLHelpers.escapeCqlString("A fine \'mess\' you\'ve gotten us into, Ollie."));
    }

    @Test
    public void testGetQueryTextSimpleSelect() throws Exception {
        assertNull(CQLHelpers.getQueryText(null));

        RegularStatement regular = QueryBuilder.select().all().from("table");
        assertEquals("SELECT * FROM table;", CQLHelpers.getQueryText(regular));
    }

    @Test
    public void testGetQueryPreparedStatements() throws Exception {
        try (TemporaryKeyspace keyspace = cassandraResource.multiRingClientManager
                .createTemporaryKeyspace("test", "testGetQueryPreparedStatements")) {
            SessionHelper session = keyspace.getSession();
            try {
                session.execute("CREATE TABLE test_table (id BIGINT, json TEXT, PRIMARY KEY (id))");
                RegularStatement withBindMarkers = QueryBuilder.select().all().from("test_table")
                        .where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

                PreparedStatement preparedStatement = session.prepare(withBindMarkers);
                assertEquals("{bound:SELECT * FROM test_table WHERE id=?;[0]}",
                        CQLHelpers.getQueryText(preparedStatement.bind(0L)));

                BatchStatement batch = new BatchStatement();
                batch.add(preparedStatement.bind(0L));
                batch.add(preparedStatement.bind(1L));
                assertEquals(
                        "{batch:{bound:SELECT * FROM test_table WHERE id=?;[0]}{bound:SELECT * FROM test_table WHERE id=?;[1]}}",
                        CQLHelpers.getQueryText(batch));
            } finally {
                session.execute("DROP TABLE test_table");
            }
        }
    }

}
