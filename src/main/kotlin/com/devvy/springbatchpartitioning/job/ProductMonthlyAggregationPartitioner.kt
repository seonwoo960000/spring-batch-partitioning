package com.devvy.springbatchpartitioning.job

import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.item.ExecutionContext
import java.time.LocalDate
import java.time.YearMonth
import java.time.format.DateTimeFormatter

open class ProductMonthlyAggregationPartitioner(
    private val jobParameters: ProductMonthlyAggregationJobParameters
): Partitioner{
    override fun partition(gridSize: Int): MutableMap<String, ExecutionContext> {
        val partitionedExecutionContext = mutableMapOf<String, ExecutionContext>()
        partitionDates()
            .forEachIndexed { index, partition ->
                val executionContext = ExecutionContext()
                executionContext.putString("startDate", partition.first)
                executionContext.putString("endDate", partition.second)
                partitionedExecutionContext["partition-$index"] = executionContext
            }
        return partitionedExecutionContext
    }

    fun partitionDates(): List<Pair<String, String>> {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val start = LocalDate.parse(jobParameters.startDate, formatter)
        val end = LocalDate.parse(jobParameters.endDate, formatter)

        var current = start
        var partitionedDates = mutableListOf<Pair<String, String>>()
        while (current.isBefore(end) || current == end) {
            val yearMonth = YearMonth.from(current)
            val partitionStartDate = yearMonth.atDay(1)
            val partitionEndDate = yearMonth.atEndOfMonth()

            partitionedDates.add(Pair(partitionStartDate.toString(), partitionEndDate.toString()))
            current = current.plusMonths(1)
        }

        return partitionedDates
    }
}
