package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.transaction.PlatformTransactionManager
import java.lang.Boolean
import java.time.LocalDate
import java.util.*


@Configuration
class ProductMonthlyAggregationJobConfiguration(
    private val jobRepository: JobRepository,
    private val platformTransactionManager: PlatformTransactionManager,
    private val entityManagerFactory: EntityManagerFactory,
    private val productMonthlyRepository: ProductMonthlyRepository,
) {

    @Value("\${spring.batch.job.chunk-size:1000}")
    lateinit var chunkSize: Integer

    @Value("\${spring.batch.job.pool-size:6}")
    lateinit var poolSize: Integer

    @Bean("productMonthlyAggregationJob")
    fun productMonthlyAggregationJob(
        @Qualifier("productMonthlyAggregationStep.manager") productMonthlyAggregationStepManager: Step
    ): Job {
        return JobBuilder("productMonthlyAggregationJob", jobRepository)
            .start(productMonthlyAggregationStepManager)
            // comment below code to prevent running jobs with same jobParameters multiple times
            .incrementer(UniqueRunIdIncrementer())
            .build()
    }

    @Bean("productMonthlyAggregationTaskPool")
    fun executor(): TaskExecutor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = poolSize.toInt()
        executor.maxPoolSize = poolSize.toInt()
        executor.setThreadNamePrefix("partition-thread")
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE)
        executor.initialize()
        return executor
    }

    @JobScope
    @Bean("productMonthlyAggregationStep.manager")
    fun productMonthlyAggregationStepManager(
        @Qualifier("productMonthlyAggregationPartitioner") partitioner: ProductMonthlyAggregationPartitioner,
        @Qualifier("productMonthlyAggregationReader") productMonthlyAggregationReader: JpaPagingItemReader<Product>,
        @Qualifier("productMonthlyAggregationWriter") productMonthlyAggregationWriter: ProductMonthlyAggregationWriter,
        @Qualifier("productMonthlyAggregationListener") productMonthlyAggregationListener: ProductMonthlyAggregationListener,
        @Qualifier("productMonthlyAggregationTaskPool") executor: TaskExecutor
    ): Step {
        val productMonthlyAggregationStep = productMonthlyAggregationStep(
            productMonthlyAggregationReader,
            productMonthlyAggregationWriter,
            productMonthlyAggregationListener
        )
        val partitionHandler = productMonthlyAggregationPartitionHandler(productMonthlyAggregationStep)
        return StepBuilder("productMonthlyAggregationStep.manager", jobRepository)
            .partitioner("productMonthlyAggregationStep", partitioner)
            .step(productMonthlyAggregationStep)
            .partitionHandler(partitionHandler)
            .build()
    }

    fun productMonthlyAggregationPartitionHandler(
        productMonthlyAggregationStep: Step,
    ): TaskExecutorPartitionHandler {
        val partitionHandler = TaskExecutorPartitionHandler()
        partitionHandler.setTaskExecutor(executor())
        partitionHandler.step = productMonthlyAggregationStep
        partitionHandler.gridSize = poolSize.toInt()
        return partitionHandler
    }

    fun productMonthlyAggregationStep(
        productMonthlyAggregationReader: JpaPagingItemReader<Product>,
        productMonthlyAggregationWriter: ProductMonthlyAggregationWriter,
        productMonthlyAggregationListener: ProductMonthlyAggregationListener,
    ): Step {
        return StepBuilder("productMonthlyAggregationStep", jobRepository)
            .chunk<Product, Product>(chunkSize.toInt(), platformTransactionManager)
            .reader(productMonthlyAggregationReader)
            .writer(productMonthlyAggregationWriter)
            .listener(productMonthlyAggregationListener)
            .build()
    }

    @StepScope
    @Bean("productMonthlyAggregationReader")
    fun productMonthlyAggregationReader(
        @Value("#{stepExecutionContext[${Common.STEP_EXECUTION_START_DATE}]}")
        stepExecutionStartDate: LocalDate,
        @Value("#{stepExecutionContext[${Common.STEP_EXECUTION_END_DATE}]}")
        stepExecutionEndDate: LocalDate
    ): JpaPagingItemReader<Product> {
        return ProductMonthlyAggregationReaderFactory(
            entityManagerFactory,
            stepExecutionStartDate,
            stepExecutionEndDate,
            chunkSize.toInt()
        )
            .productMonthlyAggregationReader()
    }

    @StepScope
    @Bean("productMonthlyAggregationWriter")
    fun productMonthlyAggregationWriter(): ProductMonthlyAggregationWriter {
        return ProductMonthlyAggregationWriter()
    }

    @StepScope
    @Bean("productMonthlyAggregationListener")
    fun productMonthlyAggregationListener(): ProductMonthlyAggregationListener {
        return ProductMonthlyAggregationListener(productMonthlyRepository)
    }

    @JobScope
    @Bean("productMonthlyAggregationPartitioner")
    fun productMonthlyAggregationPartitioner(
        @Value("#{jobParameters[${Common.JOB_PARAMETERS_START_DATE}]}")
        startDate: LocalDate,
        @Value("#{jobParameters[${Common.JOB_PARAMETERS_END_DATE}]}")
        endDate: LocalDate
    ): ProductMonthlyAggregationPartitioner {
        return ProductMonthlyAggregationPartitioner(startDate, endDate)
    }
}
