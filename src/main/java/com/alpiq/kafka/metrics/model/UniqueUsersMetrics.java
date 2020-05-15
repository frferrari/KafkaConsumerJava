package com.alpiq.kafka.metrics.model;

import com.alpiq.kafka.metrics.service.BasicMetricsService;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class UniqueUsersMetrics {
    private Timestamp tsMinute;
    private Long uniqueUsersCount;

    public void setTimestamp(String ts) throws IllegalArgumentException {
        DateFormat df = new SimpleDateFormat(BasicMetricsService.TIMESTAMP_FORMAT);
        try {
            this.tsMinute = new Timestamp(df.parse(ts).getTime());
        } catch (Exception e) {
            throw new IllegalArgumentException(ts + " is not a valid timestamp, format should be yyy-MM-dd HH:mm");
        }
    }

    public Timestamp getTimestamp() {
        return tsMinute;
    }

    public void stUniqueUsersCount(Long uniqueUsersCount) {
        this.uniqueUsersCount = uniqueUsersCount;
    }

    public Long getUniqueUsersCount() {
        return uniqueUsersCount;
    }
}
