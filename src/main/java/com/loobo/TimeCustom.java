package com.loobo;

public enum TimeCustom {
    MILLISECONDS(1),
    SECONDS(1000),
    MINUTES(SECONDS.getMillis() * 60),
    HOURS(MINUTES.getMillis() * 60),
    DAYS(HOURS.getMillis() * 24),
    WEEKS(DAYS.getMillis() * 7);

    private long millis;

    TimeCustom(long millis) {
        this.millis = millis;
    }

    public long getMillis() {
        return millis;
    }
}
