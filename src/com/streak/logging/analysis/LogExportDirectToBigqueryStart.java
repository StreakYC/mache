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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.taskqueue.TaskHandle;
import com.streak.logging.utils.AnalysisConstants;

@SuppressWarnings("serial")
public class LogExportDirectToBigqueryStart extends HttpServlet {
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {		
		String configClassName = req.getParameter(AnalysisConstants.LOGS_EXPORTER_CONFIGURATION_PARAM);
		TaskHandle th = LogExportDirectToBigqueryTask.enqueueTask(configClassName);
		
		String responseMessage = "Task not enqueued";
		if (th != null) {
			responseMessage = th.toString();
		}
		resp.getWriter().println(responseMessage);
	}
}
