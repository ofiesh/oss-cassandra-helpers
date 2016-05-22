package com.clearcapital.oss.cassandra.bundles;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.CQLHelpers;
import com.clearcapital.oss.cassandra.SessionHelper;
import com.clearcapital.oss.commands.Command;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.StackHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.datastax.driver.core.Statement;

/**
 * CassandraCommand can be used in conjunction with {@link CommandExecutor} to queue writes up for later execution.
 */
public class CassandraCommand implements Command {

    private static Logger log = LoggerFactory.getLogger(CassandraCommand.class);

    private String location;
    private SessionHelper session;
    private Statement statement;

    private final Collection<Object> debugInfo = new ArrayList<Object>();

    public String getLocation() {
        return location;
    }

    static public Builder builder(SessionHelper session) {
        // relative to *this* line of code,
        // 1 stack frames up will be whoever is saying "new CassandraBundle.builder()"
        String location = StackHelpers.getRelativeStackLocation(1);
        return new Builder(location, session);
    }

    public static class Builder {

        CassandraCommand result;

        Builder(String location, SessionHelper session) {
            result = new CassandraCommand();
            setSession(session);
            setLocation(location);
        }

        public Builder setLocation(String value) {
            result.location = value;
            return this;
        }

        public Builder setSession(SessionHelper value) {
            result.session = value;
            return this;
        }

        public CassandraCommand build() throws AssertException {
            AssertHelpers.notNull(result.location,
                    "Attempted to create a CassandraBundle without providing a location.");
            AssertHelpers.notNull(result.session,
                    "Attempted to create a CassandraBundle without providing a dseSession.");
            AssertHelpers.notNull(result.statement,
                    "Attempted to create a CassandraBundle without providing a Statement.");
            return result;
        }

        /**
         * Set the Bundle's statement.
         */
        public Builder setStatement(final Statement statement) {
            if (statement != null) {
                result.statement = statement;
            }
            return this;
        }

        /**
         * Add debug info to the bundle; if the bundle fails to execute, this will be written as a part of the log
         * message. Useful for example, for getting the values bound to a query.
         * 
         * @param object
         *            - an object which will be printed as part of the debug message when a bundle fails. Should have a
         *            useful toString() method
         */
        public Builder addDebugInfo(final Object object) {
            result.debugInfo.add(object);
            return this;
        }
    }


    protected SessionHelper getSession() {
        return session;
    }

    @Override
    public void execute() throws CommandExecutionException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Executing statement:" + CQLHelpers.getQueryText(statement));
            }
            getSession().execute(statement);
        } catch (Throwable e) {
            log.error("The following statement caused an exception, built here:" + getLocation() + "\n debugInfo:"
                    + debugInfo + "\n queryText:" + CQLHelpers.getQueryText(statement), e);
            throw new CommandExecutionException("Could not execute statements from CassandraCommand, built here:"
                    + getLocation(), e);
        }
    }
}
