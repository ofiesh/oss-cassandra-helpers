package com.clearcapital.oss.cassandra;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.configuration.RingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.replication_strategies.ReplicationStrategy;
import com.clearcapital.oss.cassandra.replication_strategies.SimpleStrategy;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public class SessionHelper {

    private static Logger log = LoggerFactory.getLogger(SessionHelper.class);

    private static int MAX_RETRIES = 3;
    private static int RETRY_DELAY = 500;

    private Session session;
    private RingConfiguration ringConfiguration;

    public SessionHelper(Session session, RingConfiguration ringConfiguration) {
        this.session = session;
        this.ringConfiguration = ringConfiguration;
    }

    public ResultSet execute(Statement statement, int maxRetries, boolean expandBatchOnFailure)
            throws CassandraException {
        int retries = 0;

        // This isn't actually an infinite loop: either session.execute() will work, or ++retries will exceed
        // maxRetries, or we'll see an InvalidQueryException
        while (true) {
            try {
                return session.execute(statement);
            } catch (NoHostAvailableException | QueryExecutionException e) {
                if (++retries >= maxRetries) {
                    throw new CassandraException(e);
                }
                log.debug("retrying query after " + e.getClass().getSimpleName() + ". Retry " + retries + " of "
                        + maxRetries + " exception:" + e);
            } catch (InvalidQueryException iqe) {
                String queryString = CQLHelpers.getQueryText(statement);
                log.debug("InvalidQueryException caught. Here is the queryString:" + queryString, iqe);
                if (expandBatchOnFailure) {
                    // This bit is helpful for figuring out *which* query is causing a problem:
                    if (statement instanceof BatchStatement) {
                        log.trace("Trying batch one statement at a time");
                        BatchStatement batch = (BatchStatement) statement;
                        Collection<Statement> children = batch.getStatements();
                        for (Statement child : children) {
                            log.trace("Attempting:" + CQLHelpers.getQueryText(child));

                            execute(child, maxRetries, false); // we intentionally allow CassandraExceptions to
                                                                        // propagate from this call.
                        }
                    }
                }
                throw new CassandraException(iqe);
            }
            try {
                Thread.sleep(RETRY_DELAY);
            } catch (InterruptedException e) {
            }
        }

    }

    public ResultSet execute(final Statement statement) throws CassandraException {
        return execute(statement, MAX_RETRIES, false);
    }

    public KeyspaceMetadata getKeyspaceInfo() {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

    public boolean tableExists(final String name) {
        TableMetadata metaData = getTableMetadata(name);
        return metaData != null;
    }

    public void createKeyspace(String keyspaceName) throws CassandraException {
        Statement statement = createKeyspaceStatement(keyspaceName,
                SimpleStrategy.builder().setReplication_factor(1).build());

        execute(statement);
    }

    public Session getSession() {
        return session;
    }

    public static SimpleStatement createKeyspaceStatement(String keyspaceName, ReplicationStrategy replicationStrategy)
            throws CassandraException {
        StringBuilder queryBuilder = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ");
        queryBuilder.append(keyspaceName);
        queryBuilder.append(" WITH REPLICATION = " + replicationStrategy.toCqlString());
        String queryString = queryBuilder.toString();
        SimpleStatement statement = new SimpleStatement(queryString);
        return statement;
    }

    public void dropTable(String tableName) throws CassandraException {
        execute(SchemaBuilder.dropTable(tableName).ifExists());
    }

    public String getLoggedKeyspace() {
        return session.getLoggedKeyspace();
    }

    public ResultSet execute(String string) throws CassandraException {
        return execute(new SimpleStatement(string));
    }

    public PreparedStatement prepare(RegularStatement value) {
        return session.prepare(value);
    }

    public TableMetadata getTableMetadata(String tableName) {
        return session.getCluster().getMetadata().getKeyspace(getLoggedKeyspace()).getTable(tableName);
    }

    public URI getSolrResourceUri(String tableName, String destName) {
        UriBuilder uriBuilder = UriBuilder.fromUri(ringConfiguration.getSolrUri());
        uriBuilder.path("resource");
        uriBuilder.path("{arg1}.{arg2}");
        uriBuilder.path(destName);
        URI uri = uriBuilder.build(getLoggedKeyspace(), tableName);
        return uri;
    }

    public URI getSolrCoreAdminUri(String tableName, String action, Map<String, String> queryParams) {
        UriBuilder uriBuilder = UriBuilder.fromUri(ringConfiguration.getSolrUri());
        uriBuilder.path("admin");
        uriBuilder.path("cores");
        uriBuilder.queryParam("action", action);
        uriBuilder.queryParam("name", "{arg1}.{arg2}");
        if (queryParams != null) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                uriBuilder.queryParam(entry.getKey(), entry.getValue());
            }
        }
        URI uri = uriBuilder.build(getLoggedKeyspace(), tableName);
        return uri;
    }

	public void dropKeyspace() {
		dropKeyspace(session.getLoggedKeyspace());
	}

	public void dropKeyspace(String keyspaceName) {
        session.execute(new SimpleStatement("DROP KEYSPACE " + keyspaceName ));
	}
}
