//package com.devvy.springbatchpartitioning
//
//import org.springframework.batch.core.repository.JobRepository
//import org.springframework.batch.test.JobRepositoryTestUtils
//import org.springframework.context.annotation.Bean
//import org.springframework.context.annotation.Configuration
//import javax.sql.DataSource
//
//@Configuration
//class JobTestConfiguration {
//
//    @Bean
//    fun jobRepositoryTestUtils(
//        dataSource: DataSource,
//        jobRepository: JobRepository
//    ): JobRepositoryTestUtils? {
//        val jobRepositoryTestUtils = JobRepositoryTestUtils()
//        jobRepositoryTestUtils.setJobRepository(jobRepository)
//        return jobRepositoryTestUtils
//    }
//}
