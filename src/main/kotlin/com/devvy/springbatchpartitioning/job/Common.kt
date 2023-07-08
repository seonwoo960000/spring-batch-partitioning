package com.devvy.springbatchpartitioning.job

import java.time.LocalDate
import java.time.format.DateTimeFormatter

open class Common(
) {
    companion object {
        const val JOB_PARAMETERS_START_DATE = "startDate"
        const val JOB_PARAMETERS_END_DATE = "endDate"

        const val STEP_EXECUTION_START_DATE = "stepExecutionStartDate"
        const val STEP_EXECUTION_END_DATE = "stepExecutionEndDate"
    }
}

fun LocalDate.formatLocalDate(): String {
    return this.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
}
