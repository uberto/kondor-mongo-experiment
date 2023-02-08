package com.gamasoft.kondor.mongo.core

import org.bson.BsonDocument
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

private object collForTest: BsonRepo() {
    override val collectionName: String = "collForTest"
    //retention... policy.. index
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

    fun createDoc(index: Int) = BsonDocument.parse(
        """{
            parentId: "$uuid"
            name: "subdoc${index}"
            index: $index
        }""".trimIndent()
    )

    val oneDocReader = mongoOperation {
        collForTest.drop()
        collForTest.addDocument(doc)
        val docs = collForTest.all()
        assertEquals(1, docs.count())
        docs.first()
    }

    val dropCollReader = mongoOperation {
        collForTest.drop()
        collForTest.all().count()
    }

    val docQueryReader = mongoOperation {
        (1..100).forEach {
            collForTest.addDocument(createDoc(it))
        }
        collForTest.find("{ index: 42 }").first()
    }

    private val mongoConnection = MongoConnection("mongodb://localhost:27017")

    private val dbName = "MongoProvTest"

    @Test
    fun `add and query doc safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val outcome = oneDocReader runOn provider
        val myDoc = outcome.orThrow()
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
        assertTrue(res.toString().contains("MongoErrorException"))
    }

    @Test
    fun `parsing query safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val myDoc = provider.tryRun(docQueryReader).orThrow()
        assertEquals(42, myDoc["index"]!!.asInt32().value)
    }
}