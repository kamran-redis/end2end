package com.redis.end2end.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleHistogram;

public class PipelineMetrics {
    private final Counter totalTransactions;
    private final Counter validTransactions;
    private final Counter invalidTransactions;
    private final Histogram processingTimeHistogram;
    private final Meter transactionsPerSecond;
    private final Counter totalProcessingErrors;

    public PipelineMetrics(RuntimeContext context) {
        this.totalTransactions = context.getMetricGroup()
            .counter("total_transactions");
        
        this.validTransactions = context.getMetricGroup()
            .counter("valid_transactions");
        
        this.invalidTransactions = context.getMetricGroup()
            .counter("invalid_transactions");
        
        this.processingTimeHistogram = context.getMetricGroup()
            .histogram("processing_time_ms", new SimpleHistogram());
        
        this.transactionsPerSecond = context.getMetricGroup()
            .meter("transactions_per_second", new MeterView(10));
        
        this.totalProcessingErrors = context.getMetricGroup()
            .counter("processing_errors");
    }

    public void incrementTotalTransactions() {
        totalTransactions.inc();
        transactionsPerSecond.markEvent();
    }

    public void incrementValidTransactions() {
        validTransactions.inc();
    }

    public void incrementInvalidTransactions() {
        invalidTransactions.inc();
    }

    public void recordProcessingTime(long milliseconds) {
        processingTimeHistogram.update(milliseconds);
    }

    public void incrementProcessingErrors() {
        totalProcessingErrors.inc();
    }
}