package com.clearcapital.oss.cassandra;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.client.ClientProtocolException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.clearcapital.oss.cassandra.annotation_processors.CassandraTableProcessor;
import com.clearcapital.oss.cassandra.annotation_processors.DemoModel;
import com.clearcapital.oss.cassandra.annotation_processors.DemoTable;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.test_support.CassandraTestResource;
import com.clearcapital.oss.commands.CommandExecutionException;
import com.clearcapital.oss.executors.ImmediateCommandExecutor;
import com.clearcapital.oss.java.exceptions.AssertException;

public class CassandraTableImplTest {
    @ClassRule
    public static CassandraTestResource cassandraResource = new CassandraTestResource(Arrays.asList("cassandra.yaml"));
    
    @Before
    public void beforeTest() throws ClientProtocolException, AssertException, CassandraException, CommandExecutionException, IOException {
    	CassandraTableProcessor.dropTableIfExists(cassandraResource.multiRingClientManager, DemoTable.class);
    	CassandraTableProcessor.tableBuilder(new ImmediateCommandExecutor(), cassandraResource.multiRingClientManager, DemoTable.class).build();
    }

	@Test
	public void testCuriouslyRecurringGenericPattern() throws Exception {		
		CassandraTableImpl<DemoTable, DemoModel> demoTable = new DemoTable(cassandraResource.multiRingClientManager);
		
		assertEquals(DemoTable.class,demoTable.getTableClass());
		assertEquals(DemoModel.class,demoTable.getModelClass());
	}
	
	@Test
	public void testCreateMethod() throws Exception {
		DemoTable demoTable = new DemoTable(cassandraResource.multiRingClientManager);

		DemoModel insertModel = DemoModel.builder().setId(1L).setUpdateId(2L).build(); 
		demoTable.insert(insertModel).execute();
		
		DemoModel readModel = demoTable.read(1L);
		assertEquals(insertModel,readModel);
	}
	
}
