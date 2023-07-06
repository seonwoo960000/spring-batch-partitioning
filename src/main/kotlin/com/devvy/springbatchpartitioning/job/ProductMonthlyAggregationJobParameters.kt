package com.devvy.springbatchpartitioning.job

import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.format.DateTimeFormatter

@Component("productMonthlyAggregationJobParameters")
@JobScope
open class ProductMonthlyAggregationJobParameters(
    @Value("#{jobParameters[${START_DATE}]}")
    val startDate: LocalDate,
    @Value("#{jobParameters[${END_DATE}]}")
    val endDate: LocalDate
) {
    companion object {
        const val START_DATE = "startDate"
        const val END_DATE = "endDate"
        val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    }

    fun startDateStr(): String {
        return startDate.format(dateTimeFormatter)
    }

    fun endDateStr(): String {
        return endDate.format(dateTimeFormatter)
    }

    override fun toString(): String {
        return "ProductMonthlyAggregationJobParameters(startDate='$startDate', endDate='$endDate')"
    }
}
