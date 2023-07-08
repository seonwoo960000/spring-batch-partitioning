package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.ProductMonthly
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import mu.KotlinLogging
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import java.time.format.DateTimeFormatter

private val logger = KotlinLogging.logger {}

open class ProductMonthlyAggregationListener(
    private val productMonthlyRepository: ProductMonthlyRepository
) : StepExecutionListener {
    private lateinit var startMonth: String
    private lateinit var endMonth: String

    override fun beforeStep(stepExecution: StepExecution) {
        val startDateParam = stepExecution.jobExecution.jobParameters.getLocalDate(Common.JOB_PARAMETERS_START_DATE)
        val endDateParam = stepExecution.jobExecution.jobParameters.getLocalDate(Common.JOB_PARAMETERS_END_DATE)
        if (startDateParam == null || endDateParam == null) {
            throw IllegalArgumentException("start date or end date is not provided in job parameters")
        }

        val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM")
        startMonth = startDateParam.format(dateTimeFormatter)
        endMonth = endDateParam.format(dateTimeFormatter)

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
