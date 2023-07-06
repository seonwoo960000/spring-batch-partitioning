package com.devvy.springbatchpartitioning.job

import java.time.LocalDate
import java.time.format.DateTimeFormatter

open class JobParametersKey(
) {
    companion object {
        const val START_DATE = "startDate"
        const val END_DATE = "endDate"
    }
}

fun LocalDate.formatLocalDate(): String {
    return this.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
}
