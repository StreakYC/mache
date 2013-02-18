package com.streak.logging.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class DeleteCompletedCloudStorageFilesTask extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
		AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
		
		String bigqueryProjectId = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM);
		String jobId = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_JOB_ID_PARAM);
		String queueName = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.QUEUE_NAME_PARAM);

		Bigquery bigquery = Bigquery.builder(HTTP_TRANSPORT, JSON_FACTORY)
				.setHttpRequestInitializer(credential)
				.setApplicationName("Streak Logs")
				.build();
		
		Job j = bigquery.jobs().get(bigqueryProjectId, jobId).execute();
		j.getConfiguration().getLoad().getSourceUris();
		
		
		if ("DONE".equals(j.getStatus().getState())) {
		
			FileService fs = FileServiceFactory.getFileService();
			List<AppEngineFile> filesForJob = new ArrayList<AppEngineFile>();
			for (String f : j.getConfiguration().getLoad().getSourceUris()) {
				if (f.contains("gs://")) {
					String filename = f.replace("gs://", "/gs/");
					AppEngineFile file = new AppEngineFile(filename);
					filesForJob.add(file);
					
					resp.getWriter().println("Deleting: " + f + ", appengine filename: " + filename);
				}
			}
			
			fs.delete(filesForJob.toArray(new AppEngineFile[0]));
		}
		else {
			resp.getWriter().println("Status was not DONE, it was " + j.getStatus().getState());
			
			// check again in 5 mins if the job is done
			String retryCountStr = req.getParameter(AnalysisConstants.RETRY_COUNT_PARAM);
			int retryCount = -1;
			if (AnalysisUtility.areParametersValid(retryCountStr)) {
				retryCount = Integer.parseInt(retryCountStr);
			}
			retryCount++;
			
			if (retryCount > 15) {
				resp.getWriter().println("Tried too many times to find job, aborting");
			}
			
			Queue taskQueue = QueueFactory.getQueue(queueName);
			taskQueue.add(
					Builder.withUrl(
							AnalysisUtility.getRequestBaseName(req) + "/deleteCompletedCloudStorageFilesTask")
						   .method(Method.GET)
						   .param(AnalysisConstants.BIGQUERY_JOB_ID_PARAM, jobId)
						   .param(AnalysisConstants.QUEUE_NAME_PARAM, queueName)
						   .param(AnalysisConstants.RETRY_COUNT_PARAM, String.valueOf(retryCount))
						   .etaMillis(System.currentTimeMillis() + 5 * 60 * 1000)
						   .param(AnalysisConstants.BIGQUERY_PROJECT_ID_PARAM, bigqueryProjectId));
		
		}
		
	}
}
