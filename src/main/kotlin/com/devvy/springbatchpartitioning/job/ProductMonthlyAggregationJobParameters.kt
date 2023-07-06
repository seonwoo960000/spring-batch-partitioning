package com.devvy.springbatchpartitioning.job

import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component("productMonthlyAggregationJobParameters")
@JobScope
open class ProductMonthlyAggregationJobParameters(
    @Value("#{jobParameters[${START_DATE}]}")
    val startDate: String,
    @Value("#{jobParameters[${END_DATE}]}")
    val endDate: String
) {
    companion object {
        const val START_DATE = "startDate"
        const val END_DATE = "endDate"
    }

    override fun toString(): String {
        return "ProductMonthlyAggregationJobParameters(startDate='$startDate', endDate='$endDate')"
    }
}
