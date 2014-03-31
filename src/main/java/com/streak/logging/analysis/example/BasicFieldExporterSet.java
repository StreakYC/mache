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

package com.streak.logging.analysis.example;

import java.util.Arrays;
import java.util.List;

import com.google.appengine.api.log.RequestLogs;
import com.streak.logging.analysis.LogsFieldExporter;
import com.streak.logging.analysis.LogsFieldExporterSet;

public class BasicFieldExporterSet implements LogsFieldExporterSet {

	@Override
	public List<LogsFieldExporter> getExporters() {
		return Arrays.asList(
				new HttpTransactionFieldExporter(),
				new InstanceFieldExporter(),
				new PerformanceFieldExporter(),
				new TimestampFieldExporter(),
				new UrlFieldExporter(),
				new UserFieldExporter(),
				new VersionFieldExporter());
	}
	
	@Override
	public boolean skipLog(RequestLogs log)
	{
		return false;
	}

	@Override
	public List<String> applicationVersionsToExport() {
		return null;
	}
	
}
