package com.streak.logging.dataflow;

import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

public class TimestampFieldExporter implements FieldExporter {
    private static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    public Object getFieldValue(JSONObject log) {
        String startTime = log.getString("startTime");

        String date;
        String fractionalSeconds;

        if (startTime.contains(".")) {
            date = startTime.split("\\.")[0];
            fractionalSeconds = startTime.split("\\.")[1];
        }
        else {
            date = startTime.substring(0, startTime.length() - 1);
            fractionalSeconds = "";
        }

        long dateMilliseconds = extractDatePortion(date);
        long microSeconds = extractFractionalSecondsPortion(fractionalSeconds);

        return dateMilliseconds * 1000 + microSeconds;
    }

    private long extractDatePortion(String date) {
        return dateFormatter.parseDateTime(date).getMillis();
    }

    private long extractFractionalSecondsPortion(String fractionalSeconds) {
        if (fractionalSeconds.equals("")) {
            return 0;
        }

        long microSeconds = Long.valueOf(fractionalSeconds.substring(0, fractionalSeconds.length() - 1));
        return microSeconds * (long) Math.pow((double) 10, (double) (7 - fractionalSeconds.length()));
    }

    @Override
    public TableFieldSchema getSchema() {
        return new TableFieldSchema().setName("timestamp").setType("INTEGER");
    }
}
