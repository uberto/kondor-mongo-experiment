package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.eq
import org.bson.BsonDocument
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class MongoConnectionTest {

    //start mongo first! localhost:27017

    fun connectToMongo(): MongoDatabase {
        val mongoClient: MongoClient = MongoClients.create("mongodb:// ")

        println(mongoClient.listDatabases())
        return mongoClient.getDatabase("test")
    }

    private val collName = "mycoll"

    fun addADoc(database: MongoDatabase, doc: BsonDocument): String {
        val collection: MongoCollection<BsonDocument> = database.getCollection(collName, BsonDocument::class.java)

// insert a document
        return collection.insertOne(doc).insertedId.asObjectId().value.toHexString()
    }


    fun allDocs(database: MongoDatabase): Sequence<BsonDocument> {
        val collection: MongoCollection<BsonDocument> = database.getCollection(collName, BsonDocument::class.java)
        return collection.find().asSequence()
    }

    fun findFizz(database: MongoDatabase): Sequence<BsonDocument> {
        val collection: MongoCollection<BsonDocument> = database.getCollection(collName, BsonDocument::class.java)
        return collection.find(
            eq("otherdata.nested.fizzbuzz", "91")
        ).asSequence()
    }

    fun findFizzParse(database: MongoDatabase): Sequence<BsonDocument> {
        val collection: MongoCollection<BsonDocument> = database.getCollection(collName, BsonDocument::class.java)
        val filter = BsonDocument.parse("""{ "otherdata.nested.fizzbuzz": "91" }""")
        return collection.find(filter).asSequence()
    }

    fun fizzbuzz(i: Int): String =
        when {
            i % 15 == 0 -> "FizzBuzz"
            i % 3 == 0 -> "Fizz"
            i % 5 == 0 -> "Buzz"
            else -> i.toString()
        }

    @Test
    fun `add and query doc`() {
        val db = connectToMongo()

        val documents = (1..100).map {
            BsonDocument.parse(
                """
            {
            prog: $it, 
            name: "document $it"
            otherdata: {
                       y: ${it * 2},
                       bool: true
                       nested: {
                               fizzbuzz: "${fizzbuzz(it)}"
                                }
                       }
            array: [1,2,3]           
            }
        """.trimIndent()
            )
        }

        assertTrue(db.name == "test")
        println("connected!")
        val ids = documents.map { addADoc(db, it) }
        println("saved!")
        println(ids)

        val docs = allDocs(db)

        println(docs.count())
//        docs.forEach { println(it) }

        val res = findFizzParse(db)
        assertTrue { res.count() > 0 }
        println("res count ${res.count()}")

        val ff = findFizz(db)

        println("fizzy count ${ff.count()}")
        println("Result is here: ${ff.joinToString()}")
    }

    @Test
    fun `drop collection`() {
        val db = connectToMongo()

        dropCollection(db, collName)
        val docs = allDocs(db)

        assertEquals(0, docs.count())
    }

    private fun dropCollection(db: MongoDatabase, collName: String) {
        db.getCollection(collName).drop()
    }

}