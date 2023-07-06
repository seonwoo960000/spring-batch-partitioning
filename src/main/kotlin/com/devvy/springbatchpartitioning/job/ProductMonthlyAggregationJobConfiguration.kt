package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.entity.Product
import com.devvy.springbatchpartitioning.entity.ProductMonthly
import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import jakarta.persistence.EntityManagerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.scope.context.StepSynchronizationManager
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
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
) {

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
            .chunk<Product, Product>(100, platformTransactionManager)
            .reader(productMonthlyAggregationReader)
            .writer(productMonthlyAggregationWriter)
            .listener(productMonthlyAggregationListener)
            .build()
    }

    @StepScope
    @Bean("productMonthlyAggregationReader")
    fun productMonthlyAggregationReader(
        @Value("#{jobParameters['startDate']}") startDate: String,
        @Value("#{jobParameters['endDate']}") endDate: String
    ): JpaPagingItemReader<Product> {
        val params = hashMapOf<String, Any>(
            "startDate" to startDate,
            "endDate" to endDate
        )

        return JpaPagingItemReaderBuilder<Product>()
            .name("productMonthlyAggregationReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString(
                """
                SELECT p 
                FROM Product p 
                WHERE p.date BETWEEN :startDate AND :endDate
                """
            )
            .parameterValues(params)
            .build()
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
}
