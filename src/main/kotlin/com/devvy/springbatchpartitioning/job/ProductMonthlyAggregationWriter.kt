package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.entity.ProductMonthly
import com.devvy.springbatchpartitioning.job.ProductMonthlyAggregationListener.Companion.PRODUCT_MONTHLY_KEY
import org.springframework.batch.core.StepExecution
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.support.AbstractItemStreamItemWriter
import org.springframework.beans.factory.annotation.Value

open class ProductMonthlyAggregationWriter : AbstractItemStreamItemWriter<Product>() {

    @Value("#{stepExecution}")
    lateinit var stepExecution: StepExecution

    private lateinit var productMonthly: ProductMonthly

    override fun write(chunk: Chunk<out Product>) {
        if (!::productMonthly.isInitialized) {
            productMonthly =
                stepExecution.executionContext[PRODUCT_MONTHLY_KEY]!! as ProductMonthly
        }

        chunk.forEach { productMonthly.add(it) }
        stepExecution.executionContext.put(PRODUCT_MONTHLY_KEY, productMonthly)
    }
}
