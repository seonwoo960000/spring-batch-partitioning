package com.devvy.springbatchpartitioning.job

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ProductMonthlyAggregationPartitionerTest {

    @Test
    fun testPartitionDates() {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val result = ProductMonthlyAggregationPartitioner(
            startDate = LocalDate.parse("2023-01-01", formatter),
            endDate = LocalDate.parse("2023-12-31", formatter)
        ).partitionDates()
        assertEquals(12, result.size)
        assertEquals("2023-01-01", result[0].first)
        assertEquals("2023-01-31", result[0].second)
        assertEquals("2023-02-01", result[1].first)
        assertEquals("2023-02-28", result[1].second)
        assertEquals("2023-03-01", result[2].first)
        assertEquals("2023-03-31", result[2].second)
        assertEquals("2023-04-01", result[3].first)
        assertEquals("2023-04-30", result[3].second)
        assertEquals("2023-05-01", result[4].first)
        assertEquals("2023-05-31", result[4].second)
        assertEquals("2023-06-01", result[5].first)
        assertEquals("2023-06-30", result[5].second)
        assertEquals("2023-07-01", result[6].first)
        assertEquals("2023-07-31", result[6].second)
        assertEquals("2023-08-01", result[7].first)
        assertEquals("2023-08-31", result[7].second)
        assertEquals("2023-09-01", result[8].first)
        assertEquals("2023-09-30", result[8].second)
        assertEquals("2023-10-01", result[9].first)
        assertEquals("2023-10-31", result[9].second)
        assertEquals("2023-11-01", result[10].first)
        assertEquals("2023-11-30", result[10].second)
        assertEquals("2023-12-01", result[11].first)
        assertEquals("2023-12-31", result[11].second)
    }
}
