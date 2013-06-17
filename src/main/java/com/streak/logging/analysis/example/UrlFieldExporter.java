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
import com.streak.logging.analysis.BigqueryFieldExporter;

public class UrlFieldExporter implements BigqueryFieldExporter {
	private static final List<String> NAMES = Arrays.asList("host", "path", "resource");
	
	String host = "";
	String path = "";
	String resource = "";
	
	@Override
	public void processLog(RequestLogs log) {
		host = log.getHost();
		resource = log.getResource();
		path = resource.indexOf("?") > -1 ? resource.substring(0, resource.indexOf("?")) : resource;
	}

	@Override
	public Object getField(String name) {
		// Since we're using string constants both places, we can use ==
		if (name == "host") {
			return host;
		}
		if (name == "path") {
			return path;
		}
		if (name == "resource") {
			return resource;
		}

		return null;
	}

	@Override
	public int getFieldCount() {
		return 3;
	}

	@Override
	public String getFieldName(int i) {
		return NAMES.get(i);
	}

	@Override
	public String getFieldType(int i) {
		return "string";
	}

}
