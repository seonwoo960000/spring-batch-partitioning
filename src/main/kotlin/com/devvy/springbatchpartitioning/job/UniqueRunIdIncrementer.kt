package com.devvy.springbatchpartitioning.job

import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer

class UniqueRunIdIncrementer: RunIdIncrementer() {
    companion object {
        private const val RUN_ID = "run.id"
    }

    override fun getNext(parameters: JobParameters?): JobParameters {
        val params = parameters ?: JobParameters()
        return JobParametersBuilder()
            .addLong(RUN_ID, (params.getLong(RUN_ID) ?: 0L) + 1L)
            .toJobParameters()
    }
}
