package com.clearcapital.oss.cassandra.annotation_processors;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.UriBuilder;

import org.apache.http.client.methods.HttpPost;
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

    public SolrCoreModifier(CommandExecutor executor, MultiRingClientManager manager, Class<?> tableClass)
            throws AssertException {
        super(executor, manager, tableClass);
        setSelf(this);
    }

    public void create() throws CommandExecutionException, AssertException, CassandraException {
        action(CREATE, null);
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
        Map<String, String> queryParams = new TreeMap<String, String>();
        queryParams.put("deleteAll", "true");
        action(UNLOAD, null);
    }

    private void reload(boolean reindex, boolean deleteAllData)
            throws CommandExecutionException, AssertException, CassandraException {
        Map<String, String> queryParams = new TreeMap<String, String>();
        queryParams.put("reindex", reindex ? "true" : "false");
        queryParams.put("deleteAll", deleteAllData ? "true" : "false");
        action(RELOAD, queryParams);
    }

    private void action(String action, Map<String, String> parameters)
            throws CommandExecutionException, AssertException, CassandraException {
        URI uri = getSession().getSolrCoreAdminUri(tableName, action, null);
        log.info("url: " + uri.toASCIIString());

        UriBuilder builder = UriBuilder.fromUri(uri);
        if (parameters != null && !parameters.isEmpty()) {
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                builder.queryParam(param.getKey(), param.getValue());
            }
        }
        // HttpGet httpGet = new HttpGet(builder.build());
        HttpPost httpPost = new HttpPost(builder.build());

        HttpCommand command = HttpCommand.builder().setRequest(httpPost).build();
        httpPost.addHeader("Accept", "application/json");
        httpPost.addHeader("Content-Type", "application/json");
        executor.addCommand(command);
    }

}