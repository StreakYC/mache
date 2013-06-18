package com.streak.datastore.analysis.builtin;

import static com.google.appengine.api.datastore.KeyFactory.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.logging.*;
import com.google.appengine.api.datastore.*;

import org.junit.*;
import com.google.appengine.tools.development.testing.*;

public class BuiltinDatastoreToBigqueryIngesterTaskTest  {

	private static final Logger LOG = Logger.getLogger( //
		BuiltinDatastoreToBigqueryIngesterTaskTest.class.getName());

	private DatastoreService ds;
	
	private LocalServiceTestHelper helper = 
		new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
	
	private static Integer operation = 1;

	@Before
	public void setUpDatastore() {
		helper.setUp();
		ds = DatastoreServiceFactory.getDatastoreService();
	}

	@After
	public void tearDownDatastore() {
		helper.tearDown();
	}

	@Test
	public void testCheckAndGetCompletedBackup() throws Exception {
		long ts = System.currentTimeMillis();
		String targetName = "current_backup" + ts;

		fixtureForBackup("a_previous_backup", new Date());
		fixtureForBackup(targetName, null);
		fixtureForBackup("z_latest_backup" + ts, new Date());

		BuiltinDatastoreToBigqueryIngesterTask task = new BuiltinDatastoreToBigqueryIngesterTask();
		assertNull(task.checkAndGetCompletedBackup(targetName));
	}

	private synchronized Entity fixtureForBackup(String name, Date completeTime) {
		Key ancestor = createKey("_AE_DatastoreAdmin_Operation", ++operation);
		Entity e = new Entity(createKey(ancestor, "_AE_Backup_Information", ++operation));
		e.setProperty("name", name);
		e.setProperty("complete_time", completeTime);
		
		ds.put(e);
		LOG.info("Added fixture for " + e.toString());
		return e;
	}
}
