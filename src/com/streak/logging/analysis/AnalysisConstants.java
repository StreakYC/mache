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

import java.util.Arrays;
import java.util.List;

public class AnalysisConstants {
	public static final List<String> SCOPES = Arrays.asList(
			"https://www.googleapis.com/auth/bigquery",
			"https://www.googleapis.com/auth/devstorage.read_write");
	
	public static final String BIGQUERY_DATASET_ID_PARAM = "bigqueryDatasetId";
	public static final String BIGQUERY_PROJECT_ID_PARAM = "bigqueryProjectId";
	public static final String BIGQUERY_TABLE_ID_PARAM = "bigqueryTableId";
	public static final String BUCKET_NAME_PARAM = "bucketName";
	public static final String START_MS_PARAM = "startMs";
	public static final String END_MS_PARAM = "endMs";
	public static final String MS_PER_TABLE_PARAM = "msPerTable";
	public static final String MS_PER_FILE_PARAM = "msPerFile";
	public static final String BIGQUERY_FIELD_EXPORTER_SET_PARAM = "bigqueryFieldExporterSet";
	public static final String QUEUE_NAME_PARAM = "queueName";
	public static final String LOG_LEVEL_PARAM = "logLevel";
	public static final String EXPORT_NAME_PARAM = "exportName";
	public static final String KINDS_TO_EXCLUDE_PARAM = "kindsToExclude";
	public static final String CLOUD_STORAGE_PATH_BASE_PARAM = "cloudStoragePathBase";
	public static final String SHARD_COUNT_PARAM = "shardCount";

	// Amount to delay each load job to avoid getting rate limited by BigQuery
	public static final long LOAD_DELAY_MS = 40000;

	// Memcache key for BigQuery rate limiting
	public static final String LAST_BIGQUERY_JOB_TIME = "lastBigqueryJobTime";

	// Memcache namespace to use for BigQuery rate limiting
	public static final String MEMCACHE_NAMESPACE = "mache";
	
	private AnalysisConstants() {	
	}
}
