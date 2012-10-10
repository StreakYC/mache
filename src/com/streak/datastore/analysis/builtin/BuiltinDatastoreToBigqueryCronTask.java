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
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.analysis.AnalysisConstants;
import com.streak.logging.analysis.AnalysisUtility;

public class BuiltinDatastoreToBigqueryCronTask extends HttpServlet {
	private static final String AH_BUILTIN_PYTHON_BUNDLE = "ah-builtin-python-bundle";
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("application/json");
		
		long timestamp = System.currentTimeMillis();
		
		String builtinDatastoreExportConfig = req.getParameter(AnalysisConstants.BUILTIN_DATASTORE_EXPORT_CONFIG);
		if (!AnalysisUtility.areParametersValid(builtinDatastoreExportConfig)) {
			resp.getWriter().write(AnalysisUtility.failureJson("Missing required param: " + AnalysisConstants.BUILTIN_DATASTORE_EXPORT_CONFIG));
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		// Instantiate the export config 
		BuiltinDatastoreExportConfiguration exporterConfig = AnalysisUtility.instantiateExportConfig(builtinDatastoreExportConfig);
		
		
		String bucketName = exporterConfig.getBucketName();
		String bigqueryDatasetId = exporterConfig.getBigqueryDatasetId();
		String bigqueryProjectId = exporterConfig.getBigqueryProjectId();
		boolean skipExport = exporterConfig.shouldSkipExportToBigquery();
		
		if (!AnalysisUtility.areParametersValid(bucketName, bigqueryProjectId) || (!skipExport && !AnalysisUtility.areParametersValid(bigqueryDatasetId))) {
			resp.getWriter().write(AnalysisUtility.failureJson("Exporter config returned null for one of the params"));
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String queueName = exporterConfig.getQueueName();
		Queue queue;
		if (!AnalysisUtility.areParametersValid(queueName)) {
			queue = QueueFactory.getDefaultQueue();
		}
		else {
			queue = QueueFactory.getQueue(queueName);
		}
		
		String backupName = AnalysisUtility.getPreBackupName(timestamp);
		
		// start the backup task
		TaskOptions t = createBackupTaskOptions(backupName, exporterConfig.getEntityKindsToExport(), bucketName);				
		queue.add(t);
		
		// for some reason the datastore admin code appends the date to the backup name even when creating programatically
		backupName = AnalysisUtility.getPostBackupName(timestamp);
		
		// start another task to do the actual import into bigquery
		if (!exporterConfig.shouldSkipExportToBigquery()) {
			BuiltinDatastoreToBigqueryIngesterTask.enqueueTask(AnalysisUtility.getRequestBaseName(req), exporterConfig, timestamp);
		}
				
		resp.getWriter().println(AnalysisUtility.successJson("successfully kicked off backup job: " + backupName + ", export to bigquery will begin once backup is complete."));
	}
	
	private TaskOptions createBackupTaskOptions(String backupName, List<String> kindsToExport, String bucketName) {
		TaskOptions t = TaskOptions.Builder.withUrl("/_ah/datastore_admin/backup.create");
		t.param("name", backupName);
		for (String kind : kindsToExport) {
			t.param("kind", kind);
		}
		t.param("filesystem", "gs");
		t.param("gs_bucket_name", bucketName);
		
		t.method(Method.GET);
		t.header("Host", BackendServiceFactory.getBackendService().getBackendAddress(AH_BUILTIN_PYTHON_BUNDLE));
		
		return t;
	}
}
