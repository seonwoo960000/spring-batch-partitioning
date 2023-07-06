package com.devvy.springbatchpartitioning.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.*

@Entity
@Table(name = "product")
class Product(
    @Id
    @Column(name = "id", nullable = false, columnDefinition = "binary(16)")
    var id: UUID = UUID.randomUUID(),
    @Column(name = "date", nullable = false)
    val date: String,
    @Column(name = "price", nullable = false)
    val price: Long = 0,
) {
    override fun toString(): String {
        return "Product(id=$id, date='$date', price=$price)"
    }
}
