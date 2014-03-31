package com.streak.logging.analysis;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.services.bigquery.model.TableSchema;
import com.streak.logging.utils.AnalysisConstants;
import com.streak.logging.utils.AnalysisUtility;
import com.streak.logging.utils.BigqueryIngester;

public class CreateLoggingTableServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		String logsExporterConfig = req.getParameter(AnalysisConstants.LOGS_EXPORTER_CONFIGURATION_PARAM);
		LogsExportConfiguration exportConfig = AnalysisUtility.instantiateLogExporterConfig(logsExporterConfig);

		long now = System.currentTimeMillis();
		long logRangeEndMs = AnalysisUtility.round(now, exportConfig.getMillisPerExport());
		long logRangeStartMs = logRangeEndMs - exportConfig.getMillisPerExport();
		
		TableSchema schema = AnalysisUtility.createSchema(exportConfig.getExporterSet());

		BigqueryIngester.createTable(exportConfig.getBigqueryProjectId(), exportConfig.getBigqueryDatasetId(), exportConfig.getBigqueryTableId(logRangeStartMs, logRangeEndMs), schema, exportConfig.getBigquery());
		BigqueryIngester.createTable(exportConfig.getBigqueryProjectId(), exportConfig.getBigqueryDatasetId(), exportConfig.getBigqueryNextTableId(logRangeStartMs, logRangeEndMs), schema, exportConfig.getBigquery());
	}
}
