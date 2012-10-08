/*
 * Copyright 2012 Rewardly Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streak.datastore.analysis.builtin;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.analysis.AnalysisConstants;
import com.streak.logging.analysis.AnalysisUtility;

public class BuiltinDatastoreToBigqueryIngesterTask extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private static final String BUILTIN_DATASTORE_TO_BIGQUERY_INGESTOR_TASK_PATH = "/builtinDatastoreToBigqueryIngestorTask";

	public static void enqueueTask(String baseUrl, BuiltinDatastoreExportConfiguration exporterConfig, long timestamp) {
		enqueueTask(baseUrl, exporterConfig, timestamp, 0);
	}	
	
	private static void enqueueTask(String baseUrl, BuiltinDatastoreExportConfiguration exporterConfig, long timestamp, long countdownMillis) {
		TaskOptions t = TaskOptions.Builder.withUrl(baseUrl + BUILTIN_DATASTORE_TO_BIGQUERY_INGESTOR_TASK_PATH);
		t.param(AnalysisConstants.TIMESTAMP_PARAM, Long.toString(timestamp));
		t.param(AnalysisConstants.BUILTIN_DATASTORE_EXPORT_CONFIG, exporterConfig.getClass().getName());
		
		t.method(Method.GET);
		if (countdownMillis > 0) {
			t.countdownMillis(countdownMillis);
		}
		QueueFactory.getQueue(exporterConfig.getQueueName()).add(t);
	}
	
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("application/json");

		String timestampStr = req.getParameter(AnalysisConstants.TIMESTAMP_PARAM);
		long timestamp = 0;
		if (AnalysisUtility.areParametersValid(timestampStr)) {
			try {
				timestamp = Long.parseLong(timestampStr);
			}
			catch (Exception e) {
				// leave it at default value
			}
		}
		if (timestamp == 0) {
			resp.getWriter().write(AnalysisUtility.failureJson("Missing required param: " + AnalysisConstants.TIMESTAMP_PARAM));
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String builtinDatastoreExportConfig = req.getParameter(AnalysisConstants.BUILTIN_DATASTORE_EXPORT_CONFIG);
		if (!AnalysisUtility.areParametersValid(builtinDatastoreExportConfig)) {
			resp.getWriter().write(AnalysisUtility.failureJson("Missing required param: " + AnalysisConstants.BUILTIN_DATASTORE_EXPORT_CONFIG));
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// Instantiate the export config 
		BuiltinDatastoreExportConfiguration exporterConfig = AnalysisUtility.instantiateExportConfig(builtinDatastoreExportConfig);
		
		if (!isBackupComplete(AnalysisUtility.getPostBackupName(timestamp))) {
			resp.getWriter().println(AnalysisUtility.successJson("backup incomplete, retrying in 1000 millis"));
			enqueueTask(AnalysisUtility.getRequestBaseName(req), exporterConfig, timestamp, 1000);
		}
		else {
			resp.getWriter().println(AnalysisUtility.successJson("backup complete, starting bigquery ingestion"));
//			AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
//			HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(credential);
//			
//			Bigquery bigquery = Bigquery.builder(HTTP_TRANSPORT, JSON_FACTORY)
//					.setHttpRequestInitializer(credential)
//					.setApplicationName("Streak Logs")
//					.build();
//			
//			
//			String datatableSuffix = Long.toString(timestamp);
//			if (replaceExistingTables) {
//				// delete the dataset
//				bigquery.datasets().delete(bigqueryDatasetId, bigqueryProjectId);
//				datatableSuffix = "";
//			}
//			
//			
//			Job job = new Job();
//			JobConfiguration config = new JobConfiguration();
//			JobConfigurationLoad loadConfig = new JobConfigurationLoad();
//			
//			loadConfig.setSourceUris(urisToProcess);
//			loadConfig.set("sourceFormat", "DATASTORE_BACKUP");
//			loadConfig.set("allowQuotedNewlines", true);
//			
//			TableSchema schema = new TableSchema();
//			// TODO(frew): Support for multiple schemas?
//			loadSchema(schemaBaseUri, schema);
//			loadConfig.setSchema(schema);
//			
//			TableReference table = new TableReference();
//			table.setProjectId(bigqueryProjectId);
//			table.setDatasetId(bigqueryDatasetId);
//			table.setTableId(bigqueryTableId);
//			loadConfig.setDestinationTable(table);
//			
//			config.setLoad(loadConfig);
//			job.setConfiguration(config);
//			Insert insert = bigquery.jobs().insert(bigqueryProjectId, job);
//			
//			// TODO(frew): Not sure this is necessary, but monkey-see'ing the example code
//			insert.setProjectId(bigqueryProjectId);
//			JobReference ref = insert.execute().getJobReference();
		}
		

		
		
		
	}


	private boolean isBackupComplete(String backupName) {
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		
		Query q = new Query("_AE_Backup_Information");
		FilterPredicate fp = new FilterPredicate("name", FilterOperator.EQUAL, backupName);
		q.setFilter(fp);
		
		PreparedQuery pq = datastore.prepare(q);
		Entity result = pq.asSingleEntity();
		
		Object completion = result.getProperty("completion_time");
		System.err.println("Backup complete: " + completion != null);
		return completion != null;
	}
}
