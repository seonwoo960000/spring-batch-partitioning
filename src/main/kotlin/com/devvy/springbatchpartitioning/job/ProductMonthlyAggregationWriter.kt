package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.entity.ProductMonthly
import org.springframework.batch.core.StepExecution
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.support.AbstractItemStreamItemWriter
import org.springframework.beans.factory.annotation.Value

open class ProductMonthlyAggregationWriter : AbstractItemStreamItemWriter<Product>() {

    @Value("#{stepExecution}")
    lateinit var stepExecution: StepExecution

    private val productMonthlies = mutableMapOf<String, ProductMonthly>()

    override fun write(chunk: Chunk<out Product>) {
        chunk.groupBy { it.month() }
            .map { (yearMonth, products) ->
                val key = ProductMonthlyKeyUtils.productMonthlyKey(yearMonth)
                val productMonthly = productMonthlies.getOrPut(key) {
                        ProductMonthly.default(yearMonth)
                    }

                products.forEach { productMonthly.add(it) }
            }

        productMonthlies.keys.forEach {
            stepExecution.executionContext.put(it, productMonthlies[it])
        }
    }
}
