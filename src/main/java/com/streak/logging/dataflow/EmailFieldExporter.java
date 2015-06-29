package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.rewardly.mailfoo.utils.Constants;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class EmailFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        JSONArray logLines = log.has("line") ? log.getJSONArray("line") : new JSONArray();
        for (int i = 0; i < logLines.length(); i++) {
            String line = logLines.getJSONObject(i).getString("logMessage");
            if (LogUtils.isLineHeadingFor(Constants.REQUEST_DATA_LOG_LINE, line)) {
                return LogUtils.extractParameter("email", line);
            }
        }

        return "";
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("email").setType("STRING");
    }
}
