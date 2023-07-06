package com.devvy.springbatchpartitioning.job

import java.time.YearMonth

class ProductMonthlyKeyUtils {
    companion object {
        const val PRODUCT_MONTHLY_KEY = "productMonthly-"

        fun productMonthlyKey(yearMonth: String): String {
            return PRODUCT_MONTHLY_KEY + yearMonth
        }

        fun productMonthlyKeys(startMonth: String, endMonth: String): List<String> {
            var start = YearMonth.parse(startMonth)
            val end = YearMonth.parse(endMonth)

            val result = mutableListOf<String>()
            while (!start.isAfter(end)) {

                result.add(PRODUCT_MONTHLY_KEY + start.toString())
                start = start.plusMonths(1)
            }

            return result
        }
    }
}
