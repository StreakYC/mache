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

package com.streak.datastore.analysis.builtin;

import java.util.List;

import com.streak.logging.utils.AnalysisUtility;

public interface BuiltinDatastoreExportConfiguration {
	/**
	 * Specifies which entity kinds should be exported to bigquery.
	 * @return a list of strings, each representing the name of the entity kind
	 */
	public List<String> getEntityKindsToExport();
	
	/**
	 * Specifies the Google cloud storage bucket that will receive the datastore backup.
	 * @return the bucket name you'd like the datastore backup stored to
	 */
	public String getBucketName();
	
	/**
	 * Specifies the BigQuery dataset in which the exported tables will be created.
	 * @return the dataset in bigquery that you'd like tables created in for this datastore export
	 */
	public String getBigqueryDatasetId();
	
	/**
	 * Specifies the x-goog-project-id of your cloud storage project.
	 * @return your project id available in the api console
	 */
	public String getBigqueryProjectId();
	
	/**
	 * Specifies whether to append a timestamp to the BigQuery table names.
	 * @return in bigquery whether or not to append the timestamp of the export to the names of the tables created in bigquery.
	 * If you want your export to overwrite previous exports in bigquery you should set this to false that way it overrides the last export.
	 */
	public boolean appendTimestampToDatatables();
	
	/**
	 * Specifies the task queue to use for this job, by default this is the default queue.
	 * @return the task queue to use for this job. Return null if you want to use default queue
	 */
	public String getQueueName();
	
	/**
	 * Return true if you want to just use mache for automatic backup creation to cloud storage and not exporting to bigquery.
	 * Normally this should be set to false. 
	 * @return whether not to export to bigquery
	 */
	public boolean shouldSkipExportToBigquery();

	/**
	* Return true if you want to run tasks as a service. Normally this should be set to true.
	* Set to false if you do not have enough permissions to run as a service.
	* If you do not have enough permissions, backup will fail with "Could not create backup via link: INSUFFICIENT_PERMISSION".
	* @return whether to run the backup as a service
	*/
	public boolean runAsService();
	
	/**
	 * This allows you to name the backups created in cloud storage and the datastore admin console for future reference 
	 * @return the name of the backup to be used in Google Cloud Storage, or null to use the default
	 **TODO: There is no default for null at the moment, see AnalysisUtility.getPreBackupName.
	 */
	public String getBackupNamePrefix();
}