package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.CassandraTableImpl;
import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;
import com.clearcapital.oss.cassandra.annotations.CassandraDataType;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.cassandra.annotations.JsonColumnInfo;
import com.clearcapital.oss.cassandra.annotations.ReflectionColumnInfo;
import com.clearcapital.oss.cassandra.annotations.SolrOptions;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;

/**
 * This is just a simple table definition suitable for using in our tests.
 * 
 * @author eehlinger
 */
@CassandraTable( // @formatter:off
        multiRingGroup = "groupB",
        modelClass = DemoModel.class,
        tableName = "testCreateTable",
        columns = {
            @Column(cassandraName = DemoSolrTable.ID_COLUMN, reflectionColumnInfo = @ReflectionColumnInfo(
                javaPath = { "id" },
                dataType = CassandraDataType.BIGINT,
                columnOption = ColumnOption.PARTITION_KEY
                )),
            @Column(cassandraName = DemoSolrTable.JSON_COLUMN, jsonColumnInfo = @JsonColumnInfo(model = Object.class)),
            @Column(cassandraName = DemoSolrTable.SOLR_QUERY_COLUMN, createdElsewhere = true) 
        },
        solrOptions = @SolrOptions(
           schemaResourceName = "test/tables/DemoSolrTable/schema.xml",
           solrconfigResourceName = "test/tables/DemoSolrTable/solrconfig.xml",
           coreCreationTimeoutMs = 10000
        )
    ) // @formatter:on
public class DemoSolrTable extends CassandraTableImpl<DemoSolrTable,DemoModel> {

    public static final String ID_COLUMN = "id";
    public static final String UPDATE_ID_COLUMN = "updateId";
    public static final String JSON_COLUMN = "json";
    public static final String FLUID_TYPE_COLUMN = "fluidType";
    public static final String EXTRA_COLUMN = "extraColumn";
    public static final String SOLR_QUERY_COLUMN = "solr_query";

    DemoSolrTable(MultiRingClientManager multiRingClientManager) {
    	super(multiRingClientManager);
    }
    
}
