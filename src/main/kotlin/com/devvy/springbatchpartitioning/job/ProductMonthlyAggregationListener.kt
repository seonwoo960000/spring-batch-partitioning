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
        if (!stepExecution.executionContext.containsKey(JobParametersKey.START_DATE) ||
            !stepExecution.executionContext.containsKey(JobParametersKey.END_DATE)) {
            throw IllegalArgumentException("start date or end date is not provided in job parameters")
        }

        startMonth = stepExecution.executionContext[JobParametersKey.START_DATE].toString().substring(0, 7)
        endMonth = stepExecution.executionContext[JobParametersKey.END_DATE].toString().substring(0, 7)

        ProductMonthlyKeyUtils.productMonthlyKeysBetween(startMonth, endMonth)
            .forEach {
                stepExecution.executionContext.put(it, ProductMonthly.default(startMonth))
            }
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
        if (stepExecution.exitStatus.equals(ExitStatus.COMPLETED)) {
            ProductMonthlyKeyUtils.productMonthlyKeysBetween(startMonth, endMonth)
                .forEach {
                    productMonthlyRepository.save(stepExecution.executionContext[it]!! as ProductMonthly)
                }
        } else {
            logger.error("Step execution failed")
        }
        return super.afterStep(stepExecution)
    }
}
