package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class ResponseSizeFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        return log.has("responseSize") ? log.getInt("responseSize") : 0L;
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("responseSize").setType("INTEGER");
    }
}
