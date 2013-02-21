package com.streak.logging.analysis;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;

public class TestLogsAccessibleServlet extends HttpServlet {

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
		String exporterSetClassStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM);
		BigqueryFieldExporterSet exporterSet = AnalysisUtility.instantiateExporterSet(exporterSetClassStr);
		
		LogService ls = LogServiceFactory.getLogService();
		LogQuery lq = new LogQuery();
		long currMillis = System.currentTimeMillis();
		lq = lq.startTimeUsec((currMillis - 5 * 60 * 1000) * 1000)
				.endTimeUsec(currMillis * 1000)
				.includeAppLogs(true);

		
		List<String> appVersions = exporterSet.applicationVersionsToExport();
		if (appVersions != null) {
			lq = lq.majorVersionIds(appVersions);
		}

		Iterable<RequestLogs> logs = ls.fetch(lq);
		
		for (RequestLogs rl : logs) {
			resp.getWriter().println(rl.getCombined() + "\n");
		}
		
	}
}
