package com.devvy.springbatchpartitioning.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.io.Serializable
import java.util.*

@Entity
@Table(name = "product_monthly")
class ProductMonthly(
    @Id
    @Column(name = "id", nullable = false, columnDefinition = "binary(16)")
    var id: UUID = UUID.randomUUID(),
    @Column(name = "`month`", nullable = false)
    val month: String,
    @Column(name = "price", nullable = false)
    var price: Long = 0,
): Serializable {
    companion object {
        fun default(month: String) = ProductMonthly(id = UUID.randomUUID(), month = month, price = 0L)
    }

    fun add(productMonthly: Product) {
        if (this.month != productMonthly.date.substring(0, 7)) {
            // @formatter:off
            throw IllegalArgumentException("Different month: ${this.month} != ${productMonthly.date.substring(0, 7)}")
        }
        this.price += productMonthly.price
    }

    override fun toString(): String {
        return "ProductMonthly(id=$id, month='$month', price=$price)"
    }
}
