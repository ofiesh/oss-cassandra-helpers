package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;
import com.clearcapital.oss.cassandra.annotations.CassandraDataType;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.ClusteringOrder;
import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.cassandra.annotations.JsonColumnInfo;
import com.clearcapital.oss.cassandra.annotations.ReflectionColumnInfo;
import com.clearcapital.oss.cassandra.annotations.table_properties.TableProperties;

/**
 * This is just a simple table definition suitable for using in our tests.
 * 
 * @author eehlinger
 */
@CassandraTable( // @formatter:off
        multiRingGroup = "groupA",
        tableName = "testCreateTable",
        columns = {
            @Column(cassandraName = DemoTable.ID_COLUMN, reflectionColumnInfo = @ReflectionColumnInfo(
                javaPath = { "id" },
                dataType = CassandraDataType.BIGINT,
                columnOption = ColumnOption.PARTITION_KEY
                )),
            @Column(cassandraName = DemoTable.UPDATE_ID_COLUMN, reflectionColumnInfo = @ReflectionColumnInfo(
                javaPath = { DemoTable.UPDATE_ID_COLUMN },
                dataType = CassandraDataType.BIGINT,
                columnOption = ColumnOption.CLUSTERING_KEY
                )),
            @Column(cassandraName = DemoTable.FLUID_TYPE_COLUMN, reflectionColumnInfo = @ReflectionColumnInfo(
                    javaPath = { DemoTable.FLUID_TYPE_COLUMN },
                    dataType = CassandraDataType.TEXT
                    )),
            @Column(cassandraName = DemoTable.JSON_COLUMN, jsonColumnInfo = @JsonColumnInfo(model = Object.class))
        },
        clusteringOrder = {
            @ClusteringOrder(columnName = DemoTable.UPDATE_ID_COLUMN, descending = true)
        },
        properties = @TableProperties(
            comment = "hello"
        )
    ) // @formatter:on
public class DemoTable {

    public static final String ID_COLUMN = "id";
    public static final String UPDATE_ID_COLUMN = "updateId";
    public static final String JSON_COLUMN = "json";
    public static final String FLUID_TYPE_COLUMN = "fluidType";
    public static final String EXTRA_COLUMN = "extraColumn";

}
