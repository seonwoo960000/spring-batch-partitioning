package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.entity.ProductMonthly
import mu.KotlinLogging
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter

private val logger = KotlinLogging.logger {}

open class ProductMonthlyAggregationWriter : ItemWriter<Product> {
    private val productMonthlies = mutableMapOf<String, ProductMonthly>()

    override fun write(chunk: Chunk<out Product>) {
        chunk.groupBy { it.month() }
            .map { (yearMonth, products) ->
                // Uncomment below code to test spring batch job restarting failed step logic
                // if (yearMonth == "2023-01") {
                //     throw IllegalArgumentException("damn!!")
                // }

                val key = ProductMonthlyKeyUtils.productMonthlyKey(yearMonth)
                val productMonthly = productMonthlies.getOrPut(key) {
                        ProductMonthly.default(yearMonth)
                    }

                products.forEach { productMonthly.add(it) }
                println("Writer ----> yearMonth: $yearMonth")
            }
    }

    @AfterStep
    fun afterStep(stepExecution: StepExecution): ExitStatus {
        if (productMonthlies.keys.size > 1) {
            logger.warn { "productMonthlies size is greater than 1. Check your partition configuration." }
        }
        productMonthlies.keys.forEach {
            stepExecution.executionContext.put(it, productMonthlies[it])
        }
        return ExitStatus.COMPLETED
    }
}
