package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.support.AbstractItemStreamItemWriter
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
            .incrementer(CustomRunIdIncrementer())
            .start(productMonthlyAggregationStepManager)
            .build()
    }

//    @Bean("productMonthlyAggregationPartitionHandler")
//    fun productMonthlyAggregationPartitionHandler(
//        @Qualifier("productMonthlyAggregationStep") productMonthlyAggregationStep: Step,
//        @Qualifier("productMonthlyAggregationTaskPool") taskExecutor: TaskExecutor
//    ): TaskExecutorPartitionHandler {
//        val partitionHandler = TaskExecutorPartitionHandler()
//        partitionHandler.step = productMonthlyAggregationStep
//        partitionHandler.setTaskExecutor(taskExecutor)
//        partitionHandler.gridSize = poolSize
//        return partitionHandler
//    }

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

    @Bean("productMonthlyAggregationStep.manager")
    fun productMonthlyAggregationStepManager(
        @Qualifier("productMonthlyAggregationPartitioner") partitioner: ProductMonthlyAggregationPartitioner,
//        @Qualifier("productMonthlyAggregationPartitionHandler") partitionHandler: TaskExecutorPartitionHandler,
        @Qualifier("productMonthlyAggregationReader") productMonthlyAggregationReader: JpaPagingItemReader<Product>,
        @Qualifier("productMonthlyAggregationWriter") productMonthlyAggregationWriter: AbstractItemStreamItemWriter<Product>,
        @Qualifier("productMonthlyAggregationListener") productMonthlyAggregationListener: ProductMonthlyAggregationListener,
        @Qualifier("productMonthlyAggregationTaskPool") executor: TaskExecutor
    ): Step {
        return StepBuilder("productMonthlyAggregationStep.manager", jobRepository)
            .partitioner("productMonthlyAggregationStep", partitioner)
            .step(
                StepBuilder("productMonthlyAggregationStep", jobRepository)
                    .chunk<Product, Product>(chunkSize.toInt(), platformTransactionManager)
                    .reader(productMonthlyAggregationReader)
                    .writer(productMonthlyAggregationWriter)
                    .listener(productMonthlyAggregationListener)
                    .build()
            )
//            .partitionHandler(partitionHandler)
            .taskExecutor(executor)
            .build()
    }

    @StepScope
    @Bean("productMonthlyAggregationReader")
    fun productMonthlyAggregationReader(
        @Value("#{stepExecutionContext[${JobParametersKey.START_DATE}]}")
        startDate: LocalDate,
        @Value("#{stepExecutionContext[${JobParametersKey.END_DATE}]}")
        endDate: LocalDate
    ): JpaPagingItemReader<Product> {
        return ProductMonthlyAggregationReaderFactory(entityManagerFactory, startDate, endDate, chunkSize.toInt())
            .productMonthlyAggregationReader()
    }

    @StepScope
    @Bean("productMonthlyAggregationWriter")
    fun productMonthlyAggregationWriter(): AbstractItemStreamItemWriter<Product> {
        return ProductMonthlyAggregationWriter()
    }

    @StepScope
    @Bean("productMonthlyAggregationListener")
    fun productMonthlyAggregationListener(): ProductMonthlyAggregationListener {
        return ProductMonthlyAggregationListener(productMonthlyRepository)
    }

    @StepScope
    @Bean("productMonthlyAggregationPartitioner")
    fun productMonthlyAggregationPartitioner(
        @Value("#{jobParameters[${JobParametersKey.START_DATE}]}")
        startDate: LocalDate,
        @Value("#{jobParameters[${JobParametersKey.END_DATE}]}")
        endDate: LocalDate
    ): ProductMonthlyAggregationPartitioner {
        return ProductMonthlyAggregationPartitioner(startDate, endDate)
    }
}
