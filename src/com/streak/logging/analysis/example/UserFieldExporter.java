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

public class UserFieldExporter implements BigqueryFieldExporter {
	public List<String> NAMES = Arrays.asList("nickname", "ip", "userAgent");
	
	private String nickname;
	private String ip;
	private String userAgent;
	
	@Override
	public void processLog(RequestLogs log) {
		nickname = log.getNickname();
		ip = log.getIp();
		userAgent = log.getUserAgent();
	}

	@Override
	public Object getField(String name) {
		if (name == "nickname") {
			return nickname;
		}
		if (name == "ip") {
			return ip;
		}
		if (name == "userAgent") {
			return userAgent;
		}
		
		return null;
	}

	@Override
	public int getFieldCount() {
		return NAMES.size();
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
