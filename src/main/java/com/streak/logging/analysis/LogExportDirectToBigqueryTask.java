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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;

import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.streak.logging.utils.AnalysisConstants;
import com.streak.logging.utils.AnalysisUtility;
import com.streak.logging.utils.BigqueryIngester;
import com.streak.logging.utils.InvalidFieldException;


@SuppressWarnings("serial")
public class LogExportDirectToBigqueryTask extends HttpServlet {

	private static final String TASK_URL = "/bqlogging/logExportDirectToBigqueryTask";

	private long MAX_BYTES_PER_POST = 1 * 1000 * 1000; // not exactly a megabyte, leave some buffer
	
	public static TaskHandle enqueueTask(String logsExporterConfigurationClassName) {
		LogsExportConfiguration config = AnalysisUtility.instantiateLogExporterConfig(logsExporterConfigurationClassName);
		
		long now = System.currentTimeMillis();
		long logRangeEndMs = AnalysisUtility.round(now, config.getMillisPerExport());
		long logRangeStartMs = logRangeEndMs - config.getMillisPerExport();
		
		
		Queue queue = QueueFactory.getQueue(config.getQueueName());
		TaskOptions t = TaskOptions.Builder.withUrl(TASK_URL);
		
		t.param(AnalysisConstants.LOGS_EXPORTER_CONFIGURATION_PARAM, logsExporterConfigurationClassName);
		t.param(AnalysisConstants.LOG_RANGE_START_MS, Long.toString(logRangeStartMs));
		t.param(AnalysisConstants.LOG_RANGE_END_MS, Long.toString(logRangeEndMs));
		
		t.method(Method.GET);
		
		String name = LogExportDirectToBigqueryTask.class.getSimpleName() + "_" + Long.toString(logRangeStartMs) + "_" + Long.toString(logRangeEndMs);
		System.out.println("exportTaskName: " + name);
		
		t.taskName(name);
		
		
		
		try {
			TaskHandle th = queue.add(t);
			System.out.println("export task enqueued");
			return th;
		}
		catch (TaskAlreadyExistsException te) {
			// we've already enqueued a task for this window, so don't worry about it
			System.out.println("export task NOT enqueued");
		}
		return null;
	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
		long logRangeStartMs = Long.parseLong(req.getParameter(AnalysisConstants.LOG_RANGE_START_MS));
		long logRangeEndMs = Long.parseLong(req.getParameter(AnalysisConstants.LOG_RANGE_END_MS));
		String logsExporterConfig = req.getParameter(AnalysisConstants.LOGS_EXPORTER_CONFIGURATION_PARAM);
		
		LogsExportConfiguration exportConfig = AnalysisUtility.instantiateLogExporterConfig(logsExporterConfig);
		LogsFieldExporterSet exporterSet = exportConfig.getExporterSet();
				
		List<LogsFieldExporter> exporters = exporterSet.getExporters();
		
		Iterable<RequestLogs> logs = queryForLogs(logRangeStartMs, logRangeEndMs, exportConfig, exporterSet);

		List<Map<String, Object>> rows = new ArrayList<>();
		List<String> insertIds = new ArrayList<>();
		
		int resultsCount = 0;
		long singleExportBytes = 2;
		
		for (RequestLogs log : logs) {
			Map<String, Object> row = new HashMap<>();
			if (exporterSet.skipLog(log)) {
				continue;
			}
			
			for (LogsFieldExporter exporter : exporters) {
				exporter.processLog(log);
				
				for (int fieldIndex = 0; fieldIndex < exporter.getFieldCount(); fieldIndex++) {					
					String fieldName = exporter.getFieldName(fieldIndex);
					String fieldType = exporter.getFieldType(fieldIndex);
					Object fieldValue = exporter.getField(fieldName);
					
					if (fieldValue == null && !exporter.getFieldNullable(fieldIndex)) {
						throw new InvalidFieldException(
								"Exporter " + exporter.getClass().getCanonicalName() + 
								" didn't return field for " + fieldName);
					}
					try {
						AnalysisUtility.putJsonValueFormatted(row, fieldName, fieldValue, fieldType);
					}
					catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			long rowBytes = row.toString().getBytes("UTF-8").length + 1; // Assumes a comma for every array item but w/e, conservative is fine
			
			
			if (singleExportBytes + rowBytes > MAX_BYTES_PER_POST) {
				System.out.println("jsonBytes: " + Long.toString(singleExportBytes));
				BigqueryIngester.streamingRowIngestion(	rows, 
														insertIds, 
														exportConfig.getBigqueryTableId(logRangeStartMs, logRangeEndMs), 
														exportConfig.getBigqueryDatasetId(), 
														exportConfig.getBigqueryProjectId(),
														exportConfig.getBigquery());
				rows = new ArrayList<Map<String, Object>>();
				insertIds = new ArrayList<>();
				singleExportBytes = 0;
			}
			
			singleExportBytes += rowBytes;
			rows.add(row);
			insertIds.add(log.getRequestId());
			
			resultsCount++;
			if (resultsCount == 19 && AnalysisUtility.isDev()) {
				break; // stupid dev server bug: https://code.google.com/p/googleappengine/issues/detail?id=8987
			}
		}
		
		System.out.println("jsonBytes: " + Long.toString(singleExportBytes));
		if (!rows.isEmpty()) {
			BigqueryIngester.streamingRowIngestion(	rows, 
													insertIds, 
													exportConfig.getBigqueryTableId(logRangeStartMs, logRangeEndMs), 
													exportConfig.getBigqueryDatasetId(), 
													exportConfig.getBigqueryProjectId(),
													exportConfig.getBigquery());
		}		
		resp.getWriter().println(AnalysisUtility.successJson(resultsCount + " rows exported"));
	}

	public Iterable<RequestLogs> queryForLogs(long logRangeStartMs, long logRangeEndMs, LogsExportConfiguration exportConfig, LogsFieldExporterSet exporterSet) {
		LogService ls = LogServiceFactory.getLogService();
		LogQuery lq = new LogQuery();
		lq = lq.startTimeUsec(logRangeStartMs * 1000)
				.endTimeUsec(logRangeEndMs * 1000)
				.includeAppLogs(true);

		if (exportConfig.getLogLevel() != null) {
			lq = lq.minLogLevel(exportConfig.getLogLevel());
		}
		
		List<String> appVersions = exporterSet.applicationVersionsToExport();
		if (appVersions != null && appVersions.size() > 0) {
			lq = lq.majorVersionIds(appVersions);
		}

		Iterable<RequestLogs> logs = ls.fetch(lq);
		return logs;
	}
}
