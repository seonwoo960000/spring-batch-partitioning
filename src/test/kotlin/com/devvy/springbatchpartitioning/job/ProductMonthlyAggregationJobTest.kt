package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.repository.ProductMonthlyRepository
import com.devvy.springbatchpartitioning.repository.ProductRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.test.JobLauncherTestUtils
import org.springframework.batch.test.JobRepositoryTestUtils
import org.springframework.batch.test.context.SpringBatchTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.jdbc.Sql
import java.time.LocalDate

@SpringBootTest
@SpringBatchTest
@TestPropertySource(properties = ["spring.batch.job.chunk-size=10", "spring.batch.job.pool-size=3"])
@Sql("classpath:/sql/dml.sql")
class ProductMonthlyAggregationJobTest(
    @Qualifier("productMonthlyAggregationJob")
    @Autowired val productMonthlyAggregationJob: Job,
    @Autowired val jobLauncher: JobLauncher,
    @Autowired val jobRepository: JobRepository,
    @Autowired val jobRepositoryTestUtils: JobRepositoryTestUtils,
    @Autowired val productRepository: ProductRepository,
    @Autowired val productMonthlyRepository: ProductMonthlyRepository,
) {

    lateinit var jobLauncherTestUtils: JobLauncherTestUtils

    @BeforeEach
    fun setUp() {
        jobLauncherTestUtils = JobLauncherTestUtils()
        jobLauncherTestUtils.job = productMonthlyAggregationJob
        jobLauncherTestUtils.jobLauncher = jobLauncher
        jobLauncherTestUtils.jobRepository = jobRepository
    }

    @Test
    fun `productMonthlyAggregationJob test with default settings`() {
        val jobParameters = JobParametersBuilder()
            .addLocalDate(
                JobParametersKey.START_DATE,
                LocalDate.parse("2023-01-01")
            )
            .addLocalDate(
                JobParametersKey.END_DATE,
                LocalDate.parse("2023-12-31")
            )
            .toJobParameters()
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)
        assertThat(jobExecution.exitStatus).isEqualTo(ExitStatus.COMPLETED)

        val productsGroupedByMonth = productRepository.findAll().groupBy { it.month() }
        val productMonthly = productMonthlyRepository.findAll().groupBy { it.month }
        assertThat(productsGroupedByMonth.size).isEqualTo(productMonthly.size)
        productsGroupedByMonth.forEach { (month, products) ->
            assertThat(products.sumOf { it.price }).isEqualTo(productMonthly[month]?.sumOf { it.price })
        }
    }

    @AfterEach
    fun cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions()
    }
}
