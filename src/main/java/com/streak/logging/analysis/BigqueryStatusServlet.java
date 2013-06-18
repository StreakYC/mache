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

package com.streak.logging.analysis;

import java.io.IOException;
import java.util.List;

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
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.ProjectList.Projects;

/**
 * Diagnostic servlet that lists visible BigQuery projects and jobs.
 */
public class BigqueryStatusServlet extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
		HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(credential);
		Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
			.setHttpRequestInitializer(credential).setApplicationName("Streak Logs").build();

		Bigquery.Projects.List projectRequest = bigquery.projects().list();
		ProjectList projectResponse = projectRequest.execute();
		resp.getWriter().println("Available Projects:" + projectResponse.toPrettyString());

		if (projectResponse.getProjects() != null) {
			for (Projects project : projectResponse.getProjects()) {
				Bigquery.Jobs.List jobsRequest = bigquery.jobs().list(project.getId());
				JobList jobsResponse = jobsRequest.execute();
				List<JobList.Jobs> jobs = jobsResponse.getJobs();
				resp.getWriter().println("=== Recent jobs for " + project.getId() + " ===");
				if (jobs != null) {
					for (JobList.Jobs job : jobs) {
						resp.getWriter().println("Job " + job.getId() + ":");
						resp.getWriter().println(job.toPrettyString());
						String jobId = job.getJobReference().getJobId();
						Bigquery.Jobs.Get jobRequest = bigquery.jobs().get(project.getId(), jobId);
						Job jobResponse = jobRequest.execute();
						resp.getWriter().println("Full job description:");
						resp.getWriter().println(jobResponse.toPrettyString());
					}
				}
			}
		}
	}
}
