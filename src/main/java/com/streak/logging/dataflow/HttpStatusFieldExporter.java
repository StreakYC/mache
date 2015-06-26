package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/25/15.
 */
public class HttpStatusFieldExporter implements FieldExporter {
    @Override
    public Integer getFieldValue(JSONObject log) {
        return log.has("status") ? log.getInt("status") : 0;
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("httpStatus").setType("INTEGER");
    }
}
