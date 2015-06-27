package com.streak.logging.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.rewardly.mailfoo.logging.*;
import com.rewardly.mailfoo.logging.Error;
import com.rewardly.mailfoo.utils.Constants;
import com.rewardly.mailfoo.utils.Utils;
import com.rewardly.mailfoo.utils.gsonutils.GsonCustom;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lordprogrammer on 6/26/15.
 */
public class ErrorsFieldExporter implements FieldExporter {
    @Override
    public Object getFieldValue(JSONObject log) {
        List<Map<String, Object>> errors = new ArrayList<>();
        JSONArray logLines = log.getJSONArray("line");

        for (int i = 0; i < logLines.length(); i++) {
            String line = logLines.getJSONObject(i).getString("logMessage");
            String logLevel = logLines.getJSONObject(i).getString("severity");

            if (LogUtils.isLineHeadingFor(Constants.STREAK_LOG_ERROR, line)) {
                String json = LogUtils.extractJson(line);
                Error e = null;
                try {
                    e = GsonCustom.getGson().fromJson(json, Error.class);
                }
                catch (Exception jse) {
//                    Logger.log("GSON ERROR");  TODO: figure out dataflow logging
//                    Logger.log(json);
                }

                if (e != null) {
                    errors.add(e.toMap());
                }
            }
            else if (logLevel.equals("ERROR") || logLevel.equals("FATAL")) {
                String message = null;
                if (Utils.areAllValid(line)) {
                    message = line.split("\n")[0];
                }
                Error e = new Error(message, line);
                errors.add(e.toMap());
            }
        }


        if (errors.isEmpty()) {
            return null;
        }
        else {
            return errors;
        }
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("errors").setType("RECORD").setMode("REPEATED").setFields(StructuredLoggable.getSchema(Error.class));
    }
}
