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

public interface BuiltinDatastoreExportConfiguration {
	/**
	 * specifies which entity kinds should be exported to bigquery
	 * @return a list of strings, each representing the name of the entity kind
	 */
	public List<String> getEntityKindsToExport();
	
	/**
	 * 
	 * @return the bucket name you'd like the datastore backup stored to
	 */
	public String getBucketName();
	
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
	 * @return in bigquery whether or not to append the timestamp of the export to the 
	 * names of the tables created in bigquery. If you wawant your export to overwrite 
	 * previous exports in bigquery you should set this to false that way it overrides the last export.
	 */
	public boolean appendTimestampToDatatables();
	
	/**
	 * 
	 * @return the task queue to use for this job. Return null if you want to use default queue
	 */
	public String getQueueName();
	
	/**
	 * Return true if you want to just use mache for automatic backup creation to cloud storage and not exporting to bigquery. Normally this should be set to false. 
	 * @return whether or not to export to bigquery
	 */
	public boolean shouldSkipExportToBigquery();
}