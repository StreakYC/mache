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

import java.util.Date;

public class BuiltinDatastoreExportStatus {
	private static final String kindName = "_MACHE_BuiltinDatastoreExportStatus_";
	
	public static final String NOT_STARTED = "notStarted";
	public static final String STARTED = "started";
	public static final String BACKUP_STARTED = "backupStarted";
	public static final String BIG_QUERY_INGESTION_STARTED = "bigqueryIngestionStarted";
	public static final String DONE = "done";
	public static final String ERROR = "error";

	private String jobId;

	private String[] kindsForExport;
	private boolean backupToCloudStorageComplete;
	private boolean[] cloudStorageToBigqueryIngestionCompleteForKind;

	private Date creationTimestamp;
	private Date lastUpdatedTimestamp;
	private String status;
	private String errorState;

	public BuiltinDatastoreExportStatus(String[] kindsForExport) {
		super();
		this.jobId = "f";
		this.kindsForExport = kindsForExport;
		this.backupToCloudStorageComplete = false;
		this.cloudStorageToBigqueryIngestionCompleteForKind = new boolean[kindsForExport.length];
		this.creationTimestamp = new Date();
		this.lastUpdatedTimestamp = this.creationTimestamp;
		this.status = this.NOT_STARTED;
		this.errorState = null;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public boolean isBackupToCloudStorageComplete() {
		return backupToCloudStorageComplete;
	}

	public void setBackupToCloudStorageComplete(
			boolean backupToCloudStorageComplete) {
		this.backupToCloudStorageComplete = backupToCloudStorageComplete;
	}

	public Date getCreationTimestamp() {
		return creationTimestamp;
	}

	public void setCreationTimestamp(Date creationTimestamp) {
		this.creationTimestamp = creationTimestamp;
	}

	public Date getLastUpdatedTimestamp() {
		return lastUpdatedTimestamp;
	}

	public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
		this.lastUpdatedTimestamp = lastUpdatedTimestamp;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getErrorState() {
		return errorState;
	}

	public void setErrorState(String errorState) {
		this.errorState = errorState;
	}

	public String[] getKindsForExport() {
		return kindsForExport;
	}

	public void setKindsForExport(String[] kindsForExport) {
		this.kindsForExport = kindsForExport;
	}

	public boolean[] getCloudStorageToBigqueryIngestionCompleteForKind() {
		return cloudStorageToBigqueryIngestionCompleteForKind;
	}

	public void setCloudStorageToBigqueryIngestionCompleteForKind(
			boolean[] cloudStorageToBigqueryIngestionCompleteForKind) {
		this.cloudStorageToBigqueryIngestionCompleteForKind = cloudStorageToBigqueryIngestionCompleteForKind;
	}

	public void persistToDatastore() throws Exception {
		throw new Exception("Not implemented yet");
	}
	
	
}
