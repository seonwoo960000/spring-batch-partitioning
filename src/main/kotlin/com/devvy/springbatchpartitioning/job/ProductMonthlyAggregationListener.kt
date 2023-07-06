package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.ProductMonthly
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener

open class ProductMonthlyAggregationListener(
    private val productMonthlyRepository: ProductMonthlyRepository
): StepExecutionListener {
    companion object {
        const val PRODUCT_MONTHLY_KEY = "productMonthly"
    }
    override fun beforeStep(stepExecution: StepExecution) {
        val startMonth = stepExecution.jobExecution.jobParameters.parameters["startDate"]?.value.toString().substring(0, 7)
        val endMonth = stepExecution.jobExecution.jobParameters.parameters["endDate"]?.value.toString().substring(0, 7)
        if (startMonth == null || endMonth == null) {
            throw IllegalArgumentException("Null month")
        }

        if (startMonth != endMonth) {
            throw IllegalArgumentException("Different month: $startMonth != $endMonth")
        }

        stepExecution.executionContext.put(PRODUCT_MONTHLY_KEY, ProductMonthly.default(startMonth))
    }

    override fun afterStep(stepExecution: StepExecution): ExitStatus? {
        if (stepExecution.exitStatus.equals(ExitStatus.COMPLETED)){
            productMonthlyRepository.save(stepExecution.executionContext["productMonthly"]!! as ProductMonthly)
        } else {
            throw IllegalStateException("Step execution failed")
        }
        return super.afterStep(stepExecution)
    }
}
