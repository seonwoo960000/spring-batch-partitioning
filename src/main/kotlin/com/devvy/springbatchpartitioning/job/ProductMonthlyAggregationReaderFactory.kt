package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder

open class ProductMonthlyAggregationReaderFactory(
    private val entityManagerFactory: EntityManagerFactory,
    private val jobParameters: ProductMonthlyAggregationJobParameters
) {

    fun productMonthlyAggregationReader(): JpaPagingItemReader<Product> {
        return JpaPagingItemReaderBuilder<Product>()
            .name("productMonthlyAggregationReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString("SELECT p FROM Product p WHERE p.date BETWEEN :startDate AND :endDate")
            .parameterValues(mapOf("startDate" to jobParameters.startDate, "endDate" to jobParameters.endDate))
            .pageSize(100)
            .build()
    }
}
