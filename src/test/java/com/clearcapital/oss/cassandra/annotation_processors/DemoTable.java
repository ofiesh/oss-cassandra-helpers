package com.clearcapital.oss.cassandra.annotation_processors;

import java.util.Map;

import com.clearcapital.oss.cassandra.CassandraTableImpl;
import com.clearcapital.oss.cassandra.ColumnDefinition.ColumnOption;
import com.clearcapital.oss.cassandra.annotations.CassandraDataType;
import com.clearcapital.oss.cassandra.annotations.CassandraTable;
import com.clearcapital.oss.cassandra.annotations.Column;
import com.clearcapital.oss.cassandra.annotations.JsonColumnInfo;
import com.clearcapital.oss.cassandra.annotations.ReflectionColumnInfo;
import com.clearcapital.oss.cassandra.annotations.table_properties.TableProperties;
import com.clearcapital.oss.cassandra.bundles.CassandraCommand;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.commands.DebuggableCommand;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.clearcapital.oss.java.exceptions.ReflectionPathException;
import com.clearcapital.oss.java.exceptions.SerializingException;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * This is just a simple table definition suitable for using in our tests.
 * 
 * @author eehlinger
 */
@CassandraTable( // @formatter:off
        multiRingGroup = "groupA", 
        tableName = "testCreateTable", 
        modelClass = DemoModel.class, 
        columns = { 
                @Column(cassandraName = DemoTable.ID_COLUMN, 
                        reflectionColumnInfo = @ReflectionColumnInfo(javaPath = { DemoTable.ID_COLUMN }, 
                        dataType = CassandraDataType.BIGINT, 
                        columnOption = ColumnOption.PARTITION_KEY)),
                @Column(cassandraName = DemoTable.UPDATE_ID_COLUMN, 
                        reflectionColumnInfo = @ReflectionColumnInfo(javaPath = { DemoTable.UPDATE_ID_COLUMN }, 
                        dataType = CassandraDataType.BIGINT, 
                        columnOption = ColumnOption.CLUSTERING_KEY_DESC)),		
                @Column(cassandraName = DemoTable.FLUID_TYPE_COLUMN, 
                        reflectionColumnInfo = @ReflectionColumnInfo(javaPath = { DemoTable.FLUID_TYPE_COLUMN }, 
                        dataType = CassandraDataType.TEXT)),
                @Column(cassandraName = DemoTable.JSON_COLUMN, 
                        jsonColumnInfo = @JsonColumnInfo(model = DemoModel.class)) }, 
        properties = @TableProperties(comment = "hello")) // @formatter:on
public class DemoTable extends CassandraTableImpl<DemoTable, DemoModel> {

    public static final String ID_COLUMN = "id";
    public static final String UPDATE_ID_COLUMN = "updateId";
    public static final String JSON_COLUMN = "json";
    public static final String FLUID_TYPE_COLUMN = "fluidType";
    public static final String EXTRA_COLUMN = "extraColumn";

    private PreparedStatement psInsert;
    private PreparedStatement psReadById;

    public DemoTable(MultiRingClientManager multiRingClientManager) throws AssertException {
        super(multiRingClientManager);

        psInsert = prepareInsertStatement(ConsistencyLevel.LOCAL_QUORUM);

        psReadById = prepareStatement(QueryBuilder.select(JSON_COLUMN).from(getTableName())
                .where(QueryBuilder.eq(ID_COLUMN, QueryBuilder.bindMarker())).limit(1), 
                ConsistencyLevel.LOCAL_QUORUM);
    }

    public DebuggableCommand insert(DemoModel value) throws AssertException, ReflectionPathException, SerializingException {
        Map<String, Object> fields = getFields(value);
        CassandraCommand result = CassandraCommand.builder(getSession())
                .setStatement(psInsert.bind(fields.values().toArray())).build();
        return result;
    }

    public DemoModel read(Long id) throws CassandraException, AssertException {
        return readIterable(psReadById.bind(id)).iterator().next();
    }

}
