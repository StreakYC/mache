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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.GSFileOptions.GSFileOptionsBuilder;
import com.google.appengine.api.files.LockException;
import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.log.LogService.LogLevel;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class StoreLogsInCloudStorageTask extends HttpServlet {
	private static final int FILE_BUFFER_LIMIT = 100000;

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setContentType("text/plain");
		
		String startMsStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.START_MS_PARAM);
		long startMs = Long.parseLong(startMsStr);
		
		String endMsStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.END_MS_PARAM);
		long endMs = Long.parseLong(endMsStr);
		
		String bucketName = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BUCKET_NAME_PARAM);
		String queueName = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.QUEUE_NAME_PARAM);
		String logLevelStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.LOG_LEVEL_PARAM);
		LogLevel logLevel = null;
		if (!"ALL".equals(logLevelStr)) {
			logLevel = LogLevel.valueOf(logLevelStr);
		}
		String exporterSetClassStr = AnalysisUtility.extractParameterOrThrow(req, AnalysisConstants.BIGQUERY_FIELD_EXPORTER_SET_PARAM);
		BigqueryFieldExporterSet exporterSet = AnalysisUtility.instantiateExporterSet(exporterSetClassStr);
		String schemaHash = AnalysisUtility.computeSchemaHash(exporterSet);
		
		List<String> fieldNames = new ArrayList<String>();
		List<String> fieldTypes = new ArrayList<String>();
		
		AnalysisUtility.populateSchema(exporterSet, fieldNames, fieldTypes);
		
		String respStr = generateExportables(startMs, endMs, bucketName, schemaHash, exporterSet, fieldNames, fieldTypes, logLevel);
		Queue taskQueue = QueueFactory.getQueue(queueName);
		taskQueue.add(
				Builder.withUrl(
						AnalysisUtility.getRequestBaseName(req) + 
						"/loadCloudStorageToBigquery?" + req.getQueryString())
					   .method(Method.GET));
		resp.getWriter().println(respStr);
	}
		
	protected String generateExportables(long startMs, long endMs, String bucketName, String schemaHash,  BigqueryFieldExporterSet exporterSet, List<String> fieldNames, List<String> fieldTypes, LogLevel logLevel) throws IOException {
		List<BigqueryFieldExporter> exporters = exporterSet.getExporters();
		
		LogService ls = LogServiceFactory.getLogService();
		LogQuery lq = new LogQuery();
		lq = lq.startTimeUsec(startMs * 1000)
			   .endTimeUsec(endMs * 1000)
			   .includeAppLogs(true);
		
		if (logLevel != null) {
			   lq = lq.minLogLevel(logLevel);
		}
		
		String fileKey = AnalysisUtility.createLogKey(schemaHash, startMs, endMs);
		String schemaKey = AnalysisUtility.createSchemaKey(schemaHash, startMs, endMs);
		
		FileService fileService = FileServiceFactory.getFileService();
		GSFileOptionsBuilder optionsBuilder = new GSFileOptionsBuilder()
		  .setBucket(bucketName)
		  .setKey(fileKey)
		  .setAcl("project-private");
		AppEngineFile logsFile = fileService.createNewGSFile(optionsBuilder.build());
		FileWriteChannel logsChannel = fileService.openWriteChannel(logsFile, true);
		PrintWriter logsWriter = new PrintWriter(Channels.newWriter(logsChannel, "UTF8"));
		Iterable<RequestLogs> logs = ls.fetch(lq);

		// Avoid scientific notation output
		NumberFormat nf = NumberFormat.getInstance();
		nf.setGroupingUsed(false);
		nf.setParseIntegerOnly(false);
		nf.setMinimumFractionDigits(1);
		nf.setMaximumFractionDigits(30);
		nf.setMinimumIntegerDigits(1);
		nf.setMaximumIntegerDigits(30);
		
		int resultsCount = 0;
		StringBuffer sb = new StringBuffer();
		for (RequestLogs log : logs) {
			int exporterStartOffset = 0;
			int currentOffset = 0;
			for (BigqueryFieldExporter exporter : exporters) {
				exporter.processLog(log);
				while (currentOffset < exporterStartOffset + exporter.getFieldCount()) {
					if (currentOffset > 0) {
						sb.append(",");
					}
					Object fieldValue = exporter.getField(fieldNames.get(currentOffset));
					if (fieldValue == null) {
						throw new InvalidFieldException(
								"Exporter " + exporter.getClass().getCanonicalName() + 
								" didn't return field for " + fieldNames.get(currentOffset));
					}
					
					// These strings have been interned so == works for comparison
					if ("string" == fieldTypes.get(currentOffset)) {
						sb.append(AnalysisUtility.escapeAndQuoteField((String) fieldValue));
					} else if ("float" == fieldTypes.get(currentOffset)) {
						sb.append(nf.format(fieldValue));
					} else {
						sb.append(fieldValue);
					}
					currentOffset++;
				}
				exporterStartOffset += exporter.getFieldCount();
			}
			sb.append("\n");
			if (sb.length() > FILE_BUFFER_LIMIT) {
				logsWriter.print(sb);
				sb.delete(0,  sb.length());
			}
			resultsCount++;
		}
		if (sb.length() > 0) {
			logsWriter.print(sb);
			sb.delete(0,  sb.length());
		}
		logsWriter.close();
		logsChannel.closeFinally();
		
		return "Saved " + resultsCount + " logs to gs://" + bucketName + "/" + fileKey;
	}
}
