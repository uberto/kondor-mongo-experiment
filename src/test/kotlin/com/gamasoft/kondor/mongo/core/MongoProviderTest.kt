package com.gamasoft.kondor.mongo.core

import org.bson.BsonDocument
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID

val collForTest = object : MongoCollection {
    override val collectionName: String = "collForTest"
}

class MongoProviderTest {

    val uuid = UUID.randomUUID()
    val doc = BsonDocument.parse(
        """{
            docId: "$uuid"
            name: "test document"
            nested: {
                       int: 42,
                       double: 3.14
                       bool: true
                     }
            array: [1,2,3,4]   
            }
        """.trimIndent()
    )

    val oneDocReader = runOnMongo {
        collForTest.addDocument(doc)
        val docs = collForTest.find()
        assertEquals(1, docs.count())
        docs.first()
    }

    val dropCollReader = runOnMongo {
        collForTest.drop()
        collForTest.find().count()
    }

    private val mongoConnection = MongoConnection("mongodb://localhost:27017")

    private val dbName = "MongoProvTest"

    @Test
    fun `add and query doc safely`() {
        val provider = MongoProvider(mongoConnection,dbName)

        val myDoc = provider.tryRun(oneDocReader).orThrow()
        assertEquals(doc, myDoc)

    }

    @Test
    fun `drop collection safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val tot: Int = provider.tryRun(dropCollReader).orThrow()
        assertEquals(0, tot)

    }

    @Test
    fun `return error in case of wrong connection`() {
        val provider = MongoProvider(MongoConnection("mongodb://localhost:12345"), dbName)

        val res = provider.tryRun(dropCollReader)
        assertTrue( res.toString().contains("MongoErrorException"))

    }
}