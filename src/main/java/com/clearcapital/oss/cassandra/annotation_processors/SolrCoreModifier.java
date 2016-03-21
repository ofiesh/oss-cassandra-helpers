package com.clearcapital.oss.cassandra.annotation_processors;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.http.HttpCommand;
import com.clearcapital.oss.java.exceptions.AssertException;

public class SolrCoreModifier extends TableProcessor<SolrCoreModifier> {

    private static Logger log = LoggerFactory.getLogger(SolrCoreModifier.class);

    private static String CREATE = "CREATE";
    private static String RELOAD = "RELOAD";
    private static String UNLOAD = "UNLOAD";

    SolrCoreModifier(CommandExecutor executor, MultiRingClientManager manager, Class<?> tableClass)
            throws AssertException {
        super(executor, manager, tableClass);
        setSelf(this);
    }

    public void create() throws CommandExecutionException, AssertException, CassandraException {
        action(CREATE);
    }

    public void reloadDontReindex() throws CommandExecutionException, AssertException, CassandraException {
        reload(false, false);
    }

    public void reloadDropIndex() throws CommandExecutionException, AssertException, CassandraException {
        reload(true, true);
    }

    public void reloadInPlace() throws CommandExecutionException, AssertException, CassandraException {
        reload(true, false);
    }

    public void unload() throws CommandExecutionException, AssertException, CassandraException {
        action(UNLOAD);
    }

    private void reload(boolean reindex, boolean deleteAllData)
            throws CommandExecutionException, AssertException, CassandraException {
        Map<String, String> queryParams = new TreeMap<String, String>();
        queryParams.put("reindex", reindex ? "true" : "false");
        queryParams.put("deleteAll", deleteAllData ? "true" : "false");
        action(RELOAD);
    }

    private void action(String action) throws CommandExecutionException, AssertException, CassandraException {
        URI uri = getSession().getSolrCoreAdminUri(tableName, action, null);
        log.info("url: " + uri.toASCIIString());

        HttpGet httpGet = new HttpGet(uri);

        HttpCommand command = HttpCommand.builder().setRequest(httpGet).build();
        executor.addCommand(command);
    }

}