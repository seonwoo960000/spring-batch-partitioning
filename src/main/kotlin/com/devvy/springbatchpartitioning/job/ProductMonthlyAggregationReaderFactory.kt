package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
import java.time.LocalDate

open class ProductMonthlyAggregationReaderFactory(
    private val entityManagerFactory: EntityManagerFactory,
    private val start: LocalDate,
    private val end: LocalDate,
    private val pageSize: Int = 1000
) {

    fun productMonthlyAggregationReader(): JpaPagingItemReader<Product> {
        val startDate = start.formatLocalDate();
        val endDate = end.formatLocalDate();
        println("Reader ----> startDate: $startDate, endDate: $endDate")

        return JpaPagingItemReaderBuilder<Product>()
            .name("productMonthlyAggregationReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString("""
                SELECT p 
                FROM Product p 
                WHERE p.date BETWEEN :startDate AND :endDate
                ORDER BY p.id
                """)
            .parameterValues(mapOf("startDate" to startDate, "endDate" to endDate))
            .pageSize(pageSize)
            .build()
    }
}
