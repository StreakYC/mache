package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class TraceFieldExporter implements FieldExporter {
    private static final int MAX_BYTES_IN_TRACE = 15 * 1024;

    @Override
    public Object getFieldValue(JSONObject log) {
        JSONArray logLines = log.has("line") ? log.getJSONArray("line") : new JSONArray();
        return LogUtils.fullTextOfLog(logLines, MAX_BYTES_IN_TRACE);
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("trace").setType("STRING");
    }
}
