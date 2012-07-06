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
