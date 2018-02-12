package com.loobo;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class EWMA implements Serializable {
    public static final double ONE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 1d);
    public static final double FIVE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 5d);
    public static final double FIFTEEN_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 15d);

    private long window;
    private long alphaWindow;
    private long last;
    private double average;
    private double alpha = -1D;
    private boolean sliding = false;

    public synchronized void mark(long time) {
        if (sliding) {
            if (time - last > window) {
                last = 0;
            }
        }

        if (last == 0) {
            this.average = 0;
            this.last = time;
        }

        long diff = time - this.last;
        double alpha = this.alpha != -1 ? this.alpha : Math.exp(-1.0 * ((double) diff / this.alphaWindow));

        this.average = (1.0 - alpha) * diff + alpha * this.average;
        this.last = time;
    }

    public double getAverageRatePer(TimeCustom timee) {
        return this.average == 0.0 ? this.average : timee.getMillis() / this.average;
    }
}















