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
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Get;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.JobList.Jobs;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.ProjectList.Projects;
import com.google.gson.Gson;
import com.streak.logging.utils.AnalysisConstants;

/**
 * Diagnostic servlet that lists visible BigQuery projects and jobs.
 */
@SuppressWarnings("serial")
public class BigqueryStatusServlet extends HttpServlet {
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("application/json");
		AppIdentityCredential credential = new AppIdentityCredential(AnalysisConstants.SCOPES);
		Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName("Streak Logs").build();

		String jobId = req.getParameter(AnalysisConstants.JOB_ID_PARAM);
		Object retVal = null;
		if (jobId == null) {
			retVal = listAllJobs(resp, bigquery);
		}
		else {
			retVal = listJob(resp, bigquery, jobId);
		}
		resp.getWriter().println(new Gson().toJson(retVal));
	}

	public List<Jobs> listAllJobs(HttpServletResponse resp, Bigquery bigquery) throws IOException {
		Bigquery.Projects.List projectRequest = bigquery.projects().list();
		ProjectList projectResponse = projectRequest.execute();

		Bigquery.Jobs.List jobsRequest = bigquery.jobs().list(projectResponse.getProjects().get(0).getId());
		JobList jobsResponse = jobsRequest.execute();
		return jobsResponse.getJobs();
	}

	public Job listJob(HttpServletResponse resp, Bigquery bigquery, String jobId) throws IOException {
		Bigquery.Projects.List projectRequest = bigquery.projects().list();
		ProjectList projectResponse = projectRequest.execute();

		if (projectResponse.getTotalItems() == 0) {
			return null;
		}

		Projects project = projectResponse.getProjects().get(0);
		Get jobsRequest = bigquery.jobs().get(project.getId(), jobId);
		Job j = jobsRequest.execute();
		return j;
	}
}