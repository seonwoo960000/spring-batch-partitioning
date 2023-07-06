package com.devvy.springbatchpartitioning.repository

import com.devvy.springbatchpartitioning.entity.ProductMonthly
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface ProductMonthlyRepository: JpaRepository<ProductMonthly, UUID> {
}
