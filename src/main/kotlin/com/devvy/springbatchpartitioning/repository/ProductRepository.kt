package com.devvy.springbatchpartitioning.repository

import com.devvy.springbatchpartitioning.entity.Product
import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface ProductRepository: JpaRepository<Product, UUID> {
}
