package com.clearcapital.oss.cassandra.annotation_processors;

import java.io.InputStream;
import java.net.URI;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.CommandExecutor;
import com.clearcapital.oss.http.HttpCommand;
import com.clearcapital.oss.java.exceptions.AssertException;

public class SolrFileUploader extends TableProcessor<SolrFileUploader> {

    private static Logger log = LoggerFactory.getLogger(SolrFileUploader.class);

    private String sourceName;
    private String destName;

    SolrFileUploader(CommandExecutor executor, MultiRingClientManager manager, Class<?> tableClass)
            throws AssertException {
        super(executor, manager, tableClass);
        setSelf(this);
    }

    public SolrFileUploader setSourceName(String value) {
        sourceName = value;
        return this;
    }

    public SolrFileUploader setDestName(String value) {
        destName = value;
        return this;
    }

    public void upload() throws CommandExecutionException, AssertException, CassandraException {
        URI uri = getSession().getSolrResourceUri(tableName, destName);
        log.info("uploadConfigFile() - url = " + uri.toASCIIString());

        InputStream is = null;
        if (is == null) {
            is = getClass().getClassLoader().getResourceAsStream(sourceName);
        }

        InputStreamEntity isEntity = new InputStreamEntity(is, ContentType.APPLICATION_XML);
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(isEntity);

        HttpCommand command = HttpCommand.builder().setRequest(httpPost).build();
        executor.addCommand(command);

        // Not sure why the old proprietary one did this...
        // byte[] bytes = IOUtils.toByteArray(is);
        // HttpEntity entity = new ByteArrayEntity(bytes, ContentType.APPLICATION_XML);
        // HttpEntity retryableEntity = new BufferedHttpEntity(entity);
        // httpPost.setEntity(retryableEntity);

    }

}