package com.clearcapital.oss.cassandra;

import java.util.Collection;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.StatementWrapperHack;

/**
 * Provides a set of methods to help in dealing with CQL.
 */
public class CQLHelpers {

    /**
     * Get the query text. This is primarily helpful when debugging, e.g., to log a query for human inspection.
     * 
     * @param statement
     *            any Cassandra statement.
     * @return
     */
    public static String getQueryText(Statement statement) {
        if (statement == null) {
            return null;
        } else if (statement instanceof RegularStatement) {
            return ((RegularStatement) statement).getQueryString();
        } else if (statement instanceof BatchStatement) {
            return getQueryText((BatchStatement) statement);
        } else if (statement instanceof BoundStatement) {
            return getQueryText((BoundStatement) statement);
        } else if (statement instanceof StatementWrapper) {
            return getQueryText((StatementWrapper) statement);
        }

        return "{" + statement.getClass().getName() + ": unrecognized statement type}";
    }

    private static String getQueryText(StatementWrapper statementWrapper) {
        StringBuilder result = new StringBuilder("{wrapped:");
        result.append(getQueryText(StatementWrapperHack.getWrappedStatement(statementWrapper)));
        result.append("}");
        return result.toString();
    }

    private static String getQueryText(BatchStatement batchStatement) {
        StringBuilder result = new StringBuilder("{batch:");

        Collection<Statement> children = batchStatement.getStatements();
        for (Statement child : children) {
            result.append(getQueryText(child));
        }

        result.append("}");
        return result.toString();
    }

    private static String getQueryText(BoundStatement boundStatement) {
        if (null == boundStatement.preparedStatement()) {
            return "Bound statement without preparedStatement.";
        }

        StringBuilder result = new StringBuilder("{bound:");
        result.append(boundStatement.preparedStatement().getQueryString());

        result.append("[");
        try {
            for (int i = 0;; ++i) {
                result.append(boundStatement.getObject(i));
            }
        } catch (Throwable t) {
            // There's no count, so we have to just keep going until we go out of bounds.
        }
        result.append("]}");
        return result.toString();
    }

    public static String escapeCqlString(final String s) {
        // escape single quotes by doubling them up
        return s.replaceAll("\'", "\'\'");
    }

}
