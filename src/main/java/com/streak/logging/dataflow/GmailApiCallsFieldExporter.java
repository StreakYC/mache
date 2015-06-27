package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.rewardly.mailfoo.logging.*;
import com.rewardly.mailfoo.utils.Constants;
import com.rewardly.mailfoo.utils.gsonutils.GsonCustom;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class GmailApiCallsFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        List<Map<String, Object>> gmailApiCalls = new ArrayList<>();
        JSONArray logLines = log.getJSONArray("line");

        for (int i = 0; i < logLines.length(); i++) {
            String line = logLines.getJSONObject(i).getString("logMessage");

            if (LogUtils.isLineHeadingFor(Constants.GMAIL_API_LOG_LINE_HEADER, line)) {
                String json = LogUtils.extractJson(line);
                GmailAPICall apiCall = GsonCustom.getGson().fromJson(json, GmailAPICall.class);
                gmailApiCalls.add(apiCall.toMap());
            }

        }

        if (gmailApiCalls.isEmpty()) {
            return null;
        } else {
            return gmailApiCalls;
        }
    }

        @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("gmailAPICalls").setType("RECORD").setMode("REPEATED").setFields(StructuredLoggable.getSchema(GmailAPICall.class));
    }
}
