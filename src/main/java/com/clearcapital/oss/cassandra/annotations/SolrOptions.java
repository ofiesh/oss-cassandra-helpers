package com.clearcapital.oss.cassandra.annotations;


public @interface SolrOptions {

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
}
