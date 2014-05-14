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

import com.google.api.services.bigquery.Bigquery;
import com.google.appengine.api.log.LogService.LogLevel;


public interface LogsExportConfiguration {
	/**
	 * 
	 * @return the dataset in bigquery that you'd like tables created in for this datastore export
	 */
	public String getBigqueryDatasetId();
	
	/**
	 * 
	 * @return your project id available in the api console
	 */
	public String getBigqueryProjectId();
	
	/**
	 * 
	 * @return the name of the the table in bigquery you want to export the logs to
	 */
	public String getBigqueryTableId(long logRangeStartMs, long logRangeEndMs);
	
	/**
	 * 
	 * @return the name of the the table in bigquery you want to export the logs to
	 */
	public String getBigqueryNextTableId(long logRangeStartMs, long logRangeEndMs);
	
	/**
	 * 
	 * @return the task queue to use for this job. Return null if you want to use default queue
	 */
	public String getQueueName();
	
	/**
	 * 
	 * @return how often to export to bigquery
	 */
	public long getMillisPerExport();
	
	/**
	 * 
	 * @return the exporter set that will be used to process log data
	 */
	public LogsFieldExporterSet getExporterSet();
	
	/**
	 * 
	 * @return the log level of the logs you want exported, return null to export all logs
	 */
	public LogLevel getLogLevel();
	
	/**
	 * 
	 * @return the a bigquery object that is authorized to access your bigquery account
	 */
	public Bigquery getBigquery();
	
	/**
	 * 
	 * @return a custom error code for export tasks that fail in case you want to do something special with reporting or retrying
	 */
	public Integer getCustomTaskFailureResponseCode();
}