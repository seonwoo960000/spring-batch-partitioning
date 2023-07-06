package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.ProductMonthly
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import mu.KotlinLogging
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener

private val logger = KotlinLogging.logger {}

open class ProductMonthlyAggregationListener(
    private val productMonthlyRepository: ProductMonthlyRepository
) : StepExecutionListener {
    private lateinit var startMonth: String
    private lateinit var endMonth: String

    override fun beforeStep(stepExecution: StepExecution) {
        startMonth =
            stepExecution.executionContext[JobParametersKey.START_DATE].toString().substring(0, 7)
        endMonth =
            stepExecution.executionContext[JobParametersKey.END_DATE].toString().substring(0, 7)
        if (startMonth == null || endMonth == null) {
            throw IllegalArgumentException("Null month")
        }

        if (startMonth != endMonth) {
            throw IllegalArgumentException("Start month and end month are not equal. startMonth: $startMonth, endMonth: $endMonth")
        }

        ProductMonthlyKeyUtils.productMonthlyKeys(startMonth, endMonth)
            .forEach {
                stepExecution.executionContext.put(it, ProductMonthly.default(startMonth))
            }
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
        if (stepExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            ProductMonthlyKeyUtils.productMonthlyKeys(startMonth, endMonth)
                .forEach {
                    productMonthlyRepository.save(stepExecution.executionContext[it]!! as ProductMonthly)
                }
        } else {
            logger.error("Step execution failed")
        }
        return super.afterStep(stepExecution)
    }
}
