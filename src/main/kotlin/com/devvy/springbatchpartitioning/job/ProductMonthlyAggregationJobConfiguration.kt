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
import org.springframework.batch.item.support.AbstractItemStreamItemWriter
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import java.util.*

@Configuration
class ProductMonthlyAggregationJobConfiguration(
    private val jobRepository: JobRepository,
    private val platformTransactionManager: PlatformTransactionManager,
    private val entityManagerFactory: EntityManagerFactory,
    private val productMonthlyRepository: ProductMonthlyRepository,
    @Qualifier("productMonthlyAggregationJobParameters")
    private val jobParameters: ProductMonthlyAggregationJobParameters,
) {

    @Value("\${spring.batch.job.chunk-size:1000}")
    private val chunkSize: Int = 1000

    @Bean("productMonthlyAggregationJob")
    fun productMonthlyAggregationJob(
        @Qualifier("productMonthlyAggregationStep") productMonthlyAggregationStep: Step
    ): Job {
        return JobBuilder("productMonthlyAggregationJob", jobRepository)
            .incrementer(CustomRunIdIncrementer())
            .start(productMonthlyAggregationStep)
            .build()
    }

    @JobScope
    @Bean("productMonthlyAggregationStep")
    fun productMonthlyAggregationStep(
        @Qualifier("productMonthlyAggregationReader") productMonthlyAggregationReader: JpaPagingItemReader<Product>,
        @Qualifier("productMonthlyAggregationWriter") productMonthlyAggregationWriter: AbstractItemStreamItemWriter<Product>,
        @Qualifier("productMonthlyAggregationListener") productMonthlyAggregationListener: ProductMonthlyAggregationListener
    ): Step {
        return StepBuilder("productMonthlyAggregationStep", jobRepository)
            .chunk<Product, Product>(chunkSize, platformTransactionManager)
            .reader(productMonthlyAggregationReader)
            .writer(productMonthlyAggregationWriter)
            .listener(productMonthlyAggregationListener)
            .build()
    }

    @StepScope
    @Bean("productMonthlyAggregationReader")
    fun productMonthlyAggregationReader(): JpaPagingItemReader<Product> {
        return ProductMonthlyAggregationReaderFactory(entityManagerFactory, jobParameters, chunkSize)
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
    @Bean("productMonthlyPartitioner")
    fun productMonthlyPartitioner(
        @Qualifier("productMonthlyAggregationJobParameters")
        jobParameters: ProductMonthlyAggregationJobParameters
    ): ProductMonthlyAggregationPartitioner {
        return ProductMonthlyAggregationPartitioner(jobParameters)
    }
}
