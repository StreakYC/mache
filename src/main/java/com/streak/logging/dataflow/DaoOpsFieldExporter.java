package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.rewardly.mailfoo.logging.GmailAPICall;
import com.rewardly.mailfoo.logging.StructuredLoggable;
import com.rewardly.mailfoo.monitoring.DAOOperation;
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
public class DaoOpsFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        List<Map<String, Object>> daoOps = new ArrayList<>();
        JSONArray logLines = log.getJSONArray("line");

        for (int i = 0; i < logLines.length(); i++) {
            String line = logLines.getJSONObject(i).getString("logMessage");

            if (LogUtils.isLineHeadingFor(Constants.DAO_OPERATION_MONITOR_LOG_LINE, line)) {
                String json = LogUtils.extractJson(line);
                DAOOperation daoOp = GsonCustom.getGson().fromJson(json, DAOOperation.class);
                if (daoOp != null) {
                    daoOps.add(daoOp.toMap());
                }
            }

        }

        if (daoOps.isEmpty()) {
            return null;
        } else {
            return daoOps;
        }
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("daoOps").setType("RECORD").setMode("REPEATED").setFields(StructuredLoggable.getSchema(DAOOperation.class));
    }
}
