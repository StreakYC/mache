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

import java.util.List;

import com.google.appengine.api.log.LogQuery.Version;
import com.google.appengine.api.log.RequestLogs;

/**
 * BigqueryFieldExporterSet holds a List of BigqueryFieldExporters.
 * 
 * Its primary purpose is to allow for specification of a set of exporters to
 * determine what to export. Implementations must have a functional default
 * constructor.
 */
public interface LogsFieldExporterSet {
	/**
	 * Get the exporters in this BigqueryFieldExporterSet.
	 * 
	 * @return the exporters in the set
	 */
	public List<LogsFieldExporter> getExporters();

	/**
	 * Let custom exporters to filter logs
	 * 
	 * @param log
	 * @return true if given log request should be skipped
	 */
	public boolean skipLog(RequestLogs log);

	/**
	 * This method allows you to customize which application versions logs you
	 * want exported. The logs API requires us to specify a list of application
	 * versions. This corresponds to the "major" version. See the logs api docs
	 * 
	 * @return a list of "major" application versions or null if you want to just
	 *         export logs for the application this task is currently running on
	 */
	public List<Version> applicationVersionsToExport();

}
