package com.streak.logging.dataflow;

import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

public class TimestampFieldExporter implements LogsFieldExporter {
    private long timestamp;

    @Override
    public void processLog(JSONObject log) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

        String startTime = log.getString("startTime");
        String dataTime = startTime.split("\\.")[0];
        String fractionalSeconds = startTime.split("\\.")[1];



        long dateMilliseconds = formatter.parseDateTime(dataTime).getMillis();

        long microSeconds = Long.valueOf(fractionalSeconds.substring(0, fractionalSeconds.length() - 1));
        microSeconds = microSeconds * (long) Math.pow((double) 10, (double) (7 - fractionalSeconds.length()));

        timestamp = dateMilliseconds * 1000 + microSeconds;
    }

    @Override
    public Object getField(String name) {
        return timestamp;
    }

    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    public String getFieldName(int i) {
        return "timestamp";
    }

    @Override
    public String getFieldType(int i) {
        return "INTEGER";
    }

    @Override
    public boolean getFieldNullable(int i) {
        return false;
    }

    @Override
    public boolean getFieldRepeated(int i) {
        return false;
    }

    @Override
    public String getFieldMode(int fieldIndex) {
        return "REQUIRED";
    }

    @Override
    public List<TableFieldSchema> getFieldFields(int i) {
        return null;
    }

}
