package com.clearcapital.oss.cassandra.annotations;


public @interface SolrTableConfigFile {

    /**
     * Name of the file in the caller's .jar
     */
    public String resourceFileName();

    /**
     * Name the file should have during upload to solr.
     */
    public String solrFileName();
}
