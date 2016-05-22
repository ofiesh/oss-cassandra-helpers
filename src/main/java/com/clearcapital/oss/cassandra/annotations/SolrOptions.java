package com.clearcapital.oss.cassandra.annotations;

public @interface SolrOptions {

    /**
     * It may seem weird that this is defaulted to true. That's so if you add a SolrOptions annotation to your
     * CassandraTable, you don't have to say it's enabled. If you look at {@link CassandraTable}, you'll see that it
     * provides a SolrOptions that has enabled=false.
     */
    boolean enabled() default true;

    /**
     * The path to the resource file to use for schema.xml
     */
    String schemaResourceName() default "";

    /**
     * The path to the resource to use for solrconfig.xml
     */
    String solrconfigResourceName() default "";

    /**
     * Additional paths to upload with schema.xml
     */
    SolrTableConfigFile[] additionalSchemaFiles() default {};

    /**
     * Number of milliseconds to wait before throwing an exception because the solr_query field didn't show up in
     * Cassandra.
     */
    long coreCreationTimeoutMs() default 5000;
}
