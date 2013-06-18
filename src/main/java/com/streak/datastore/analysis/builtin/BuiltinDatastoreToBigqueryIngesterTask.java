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
import java.util.Arrays;
import java.util.List;

import java.util.logging.Logger;
import java.util.logging.Level;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.analysis.AnalysisConstants;
import com.streak.logging.analysis.AnalysisUtility;

public class BuiltinDatastoreToBigqueryIngesterTask extends HttpServlet {
	private static final int MILLIS_TO_ENQUEUE = 5000;
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private static final String BUILTIN_DATASTORE_TO_BIGQUERY_INGESTOR_TASK_PATH = "/builtinDatastoreToBigqueryIngestorTask";

	private static final Logger LOG = Logger.getLogger(BuiltinDatastoreToBigqueryIngesterTask.class.getName());

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
		
		String keyOfCompletedBackup = checkAndGetCompletedBackup(AnalysisUtility.getPreBackupName(timestamp, exporterConfig.getBackupNamePrefix())); 
		if (keyOfCompletedBackup == null) {
			LOG.fine("Rescheduling verification ...");
			resp.getWriter().println(AnalysisUtility.successJson("backup incomplete, retrying in " + MILLIS_TO_ENQUEUE + " millis"));
			enqueueTask(AnalysisUtility.getRequestBaseName(req), exporterConfig, timestamp, MILLIS_TO_ENQUEUE);
		}
		else {
			LOG.fine("Starting ingestion jobs ...");
			resp.getWriter().println(AnalysisUtility.successJson("backup complete, starting bigquery ingestion"));
			AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
			HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(credential);
			
			Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
					.setApplicationName("Streak Logs")
					.build();
			
			
			String datatableSuffix = "";
			if (exporterConfig.appendTimestampToDatatables()) {
				datatableSuffix = Long.toString(timestamp);
			}
			else {
				datatableSuffix = "";
			}
			
			if (!exporterConfig.appendTimestampToDatatables()) {
				// we aren't appending the timestamps so delete the old tables if they exist
				for (String kind : exporterConfig.getEntityKindsToExport()) {
					boolean found = true;
					Table t;
					try {
						t = bigquery.tables().get(exporterConfig.getBigqueryProjectId(), exporterConfig.getBigqueryDatasetId(), kind).execute();
					}
					catch (IOException e) {
						// TODO: Validate response. Only shoud use IOException if error is 404
						// Errors 403 (forbidden), for instance, will be caught here erroneously
						LOG.log(Level.INFO, "Table not found", e);
						// table not found so don't need to do anything
						found = false;
					}
						
					if (found) {
						bigquery.tables().delete(exporterConfig.getBigqueryProjectId(), exporterConfig.getBigqueryDatasetId(), kind).execute();
					}
				}
			}
			
			// now create the ingestion
			for (String kind : exporterConfig.getEntityKindsToExport()) {
				Job job = new Job();
				JobConfiguration config = new JobConfiguration();
				JobConfigurationLoad loadConfig = new JobConfigurationLoad();	
				
				String uri = "gs://" + exporterConfig.getBucketName() + "/" + keyOfCompletedBackup + "." + kind + ".backup_info";
				
				loadConfig.setSourceUris(Arrays.asList(uri));
				loadConfig.set("sourceFormat", "DATASTORE_BACKUP");
				loadConfig.set("allowQuotedNewlines", true);
				
				
				TableReference table = new TableReference();
				table.setProjectId(exporterConfig.getBigqueryProjectId());
				table.setDatasetId(exporterConfig.getBigqueryDatasetId());
				table.setTableId(kind + datatableSuffix);
				loadConfig.setDestinationTable(table);

				config.setLoad(loadConfig);
				job.setConfiguration(config);
				Insert insert = bigquery.jobs().insert(exporterConfig.getBigqueryProjectId(), job);
				
				// TODO(frew): Not sure this is necessary, but monkey-see'ing the example code
				insert.setProjectId(exporterConfig.getBigqueryProjectId());
				JobReference ref = insert.execute().getJobReference();
				
				Queue taskQueue = QueueFactory.getQueue(exporterConfig.getQueueName());
				taskQueue.add(
						Builder.withUrl(
								AnalysisUtility.getRequestBaseName(req) + "/deleteCompletedCloudStorageFilesTask")
							   .method(Method.GET)
							   .param(AnalysisConstants.BIGQUERY_JOB_ID_PARAM, ref.getJobId())
							   .param(AnalysisConstants.QUEUE_NAME_PARAM, exporterConfig.getQueueName())
							   .param(AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM, exporterConfig.getBigqueryProjectId()));
				
			}
		}
	}


	String checkAndGetCompletedBackup(String backupName) throws IOException {
		System.err.println("backupName: " + backupName);
		
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		
		Query q = new Query("_AE_Backup_Information");
		
		// for some reason the datastore admin code appends the date to the backup name even when creating programatically, 
		// so test for greater than or equal to and then take the first result
		Query.Filter greatherThanOrEqual = new FilterPredicate("name",
			FilterOperator.GREATER_THAN_OR_EQUAL, backupName);
		Query.Filter lessThan = new FilterPredicate("name", FilterOperator.LESS_THAN,
		backupName + Character.MAX_VALUE);
		q.setFilter(Query.CompositeFilterOperator.and(greatherThanOrEqual, lessThan));
		
		PreparedQuery pq = datastore.prepare(q);
		List<Entity> results = pq.asList(FetchOptions.Builder.withLimit(1));
		if (results.size() != 1) {
			throw new IOException("fatal error - can't find the datastore backup entity, big trouble");
		}
		Entity result = results.get(0);
		
		Object completion = result.getProperty("complete_time");
		String keyResult = null;
		if (completion != null) {
			keyResult = KeyFactory.keyToString(result.getKey());
		}
		
		System.err.println("result: " + result);
		System.err.println("complete_time: " + completion);
		System.err.println("Backup complete: " + completion != null);
		System.err.println("keyResult: " + keyResult);
		return keyResult;
	}
}
