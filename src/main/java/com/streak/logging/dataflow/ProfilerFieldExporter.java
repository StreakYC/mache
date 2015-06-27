package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.rewardly.mailfoo.utils.Constants;
import com.rewardly.mailfoo.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class ProfilerFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        JSONArray logLines = log.getJSONArray("line");
        for (int i = 0; i < logLines.length(); i++) {
            String line = logLines.getJSONObject(i).getString("logMessage");
            if (LogUtils.isLineHeadingFor(Constants.PROFILER_LOG_LINE, line)) {
                return Utils.getStringWithoutFirstLine(line);
            }
        }

        return "";
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("profiler").setType("STRING");
    }
}
