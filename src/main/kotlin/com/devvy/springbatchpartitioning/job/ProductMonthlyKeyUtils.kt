package com.devvy.springbatchpartitioning.job

import java.time.YearMonth

class ProductMonthlyKeyUtils {
    companion object {
        const val PRODUCT_MONTHLY_KEY = "productMonthly-"

        fun productMonthlyKey(yearMonth: String): String {
            return PRODUCT_MONTHLY_KEY + yearMonth
        }

        fun productMonthlyKeysBetween(start: String, end: String): List<String> {
            var startMonth = YearMonth.parse(start)
            val endMonth = YearMonth.parse(end)

            val result = mutableListOf<String>()
            while (!startMonth.isAfter(endMonth)) {
                result.add(PRODUCT_MONTHLY_KEY + startMonth.toString())
                startMonth = startMonth.plusMonths(1)
            }

            return result
        }
    }
}
