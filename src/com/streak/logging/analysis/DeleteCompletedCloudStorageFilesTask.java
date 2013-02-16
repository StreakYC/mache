package com.streak.logging.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.services.bigquery.model.Job;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;

public class DeleteCompletedCloudStorageFilesTask extends HttpServlet {
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
//		Job j = bigquery.jobs().get(bigqueryProjectId, jobId).execute();
//		j.getConfiguration().getLoad().getSourceUris();
//		j.getStatus().getState();
//		
//		FileService fs = FileServiceFactory.getFileService();
//		List<AppEngineFile> filesForJob = new ArrayList<AppEngineFile>();
//		for (String f : j.getConfiguration().getLoad().getSourceUris()) {
//			if (f.contains("gs://")) {
//				String filename = f.replace("gs://", "/gs/");
//				AppEngineFile file = new AppEngineFile(filename);
//				filesForJob.add(file);
//			}
//		}
//		fs.delete(filesForJob.toArray(new AppEngineFile[0]));
		
	}
}
