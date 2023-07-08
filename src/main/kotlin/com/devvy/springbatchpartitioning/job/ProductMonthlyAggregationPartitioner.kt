package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.job.Common.Companion.STEP_EXECUTION_END_DATE
import com.devvy.springbatchpartitioning.job.Common.Companion.STEP_EXECUTION_START_DATE
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ExecutionContext
import java.time.LocalDate
import java.time.YearMonth

open class ProductMonthlyAggregationPartitioner(
    private val jobParametersStartDate: LocalDate,
    private val jobParametersEndDate: LocalDate
) : Partitioner {
    override fun partition(gridSize: Int): MutableMap<String, ExecutionContext> {
        val partitionedExecutionContext = mutableMapOf<String, ExecutionContext>()
        partitionDates().forEachIndexed { index, partition ->
            val executionContext = ExecutionContext()
            executionContext.putString(STEP_EXECUTION_START_DATE, partition.first)
            executionContext.putString(STEP_EXECUTION_END_DATE, partition.second)
            partitionedExecutionContext["partition-$index"] = executionContext
        }
        return partitionedExecutionContext
    }

    fun partitionDates(): List<Pair<String, String>> {
        val start = jobParametersStartDate
        val end = jobParametersEndDate

        var current = start
        var partitionedDates = mutableListOf<Pair<String, String>>()
        while (current.isBefore(end) || current == end) {
            val yearMonth = YearMonth.from(current)
            val partitionStartDate = maxOf(start, yearMonth.atDay(1))
            val partitionEndDate = yearMonth.atEndOfMonth()

            partitionedDates.add(Pair(partitionStartDate.toString(), partitionEndDate.toString()))
            current = current.plusMonths(1)
        }

        return partitionedDates
    }
}
