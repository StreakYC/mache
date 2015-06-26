package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.json.JSONObject;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class PendingTimeUsecFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        return log.has("pendingTime") ? getUsecFromSecondsString(log.getString("pendingTime")) : 0L;
    }

    private long getUsecFromSecondsString(String seconds) {
        seconds = seconds.replace("s", "");
        double stupidVariable = Double.valueOf(seconds);
        return (long) (stupidVariable * 1000000);
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("pendingTimeUsec").setType("INTEGER");
    }
}
