package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
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
        @Qualifier("productMonthlyAggregationStep") productMonthlyAggregationStep: Step
    ): Job {
        return JobBuilder("productMonthlyAggregationJob", jobRepository)
            .start(productMonthlyAggregationStep)
            // comment below code to prevent running jobs with same jobParameters multiple times
            .incrementer(UniqueRunIdIncrementer())
            .build()
    }

    @JobScope
    @Bean("productMonthlyAggregationStep")
    fun productMonthlyAggregationStep(
        @Qualifier("productMonthlyAggregationReader") productMonthlyAggregationReader: JpaPagingItemReader<Product>,
        @Qualifier("productMonthlyAggregationWriter") productMonthlyAggregationWriter: ProductMonthlyAggregationWriter,
        @Qualifier("productMonthlyAggregationListener") productMonthlyAggregationListener: ProductMonthlyAggregationListener,
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
        @Value("#{jobParameters[${Common.JOB_PARAMETERS_START_DATE}]}")
        startDate: LocalDate,
        @Value("#{jobParameters[${Common.JOB_PARAMETERS_END_DATE}]}")
        endDate: LocalDate
    ): JpaPagingItemReader<Product> {
        return ProductMonthlyAggregationReaderFactory(
            entityManagerFactory,
            startDate,
            endDate,
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
}
