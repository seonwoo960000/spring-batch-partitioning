package com.devvy.springbatchpartitioning.job

import com.devvy.springbatchpartitioning.job.ProductMonthlyKeyUtils.Companion.PRODUCT_MONTHLY_KEY
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ProductMonthlyKeyUtilsTest {

    @Test
    fun `productMonthlyKeys test`() {
        val result = ProductMonthlyKeyUtils.productMonthlyKeys("2023-01", "2023-06")
        assertThat(result.size).isEqualTo(6)
        assertThat(result[0]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-01")
        assertThat(result[1]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-02")
        assertThat(result[2]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-03")
        assertThat(result[3]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-04")
        assertThat(result[4]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-05")
        assertThat(result[5]).isEqualTo(PRODUCT_MONTHLY_KEY + "2023-06")
    }
}
