package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder

open class ProductMonthlyAggregationReaderFactory(
    private val entityManagerFactory: EntityManagerFactory,
    private val jobParameters: ProductMonthlyAggregationJobParameters,
    private val pageSize: Int = 1000
) {

    fun productMonthlyAggregationReader(): JpaPagingItemReader<Product> {
        val startDate = jobParameters.startDateStr()
        val endDate = jobParameters.endDateStr()
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
