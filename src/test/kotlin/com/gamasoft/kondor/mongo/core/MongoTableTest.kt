package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import com.ubertob.kondortools.expectSuccess
import org.bson.BsonDocument
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isEqualTo
import java.time.Duration

class MongoTableTest {

    object simpleDocTable : TypedTable<SmallClass>(JSmallClass) {
        override val collectionName: String = "simpleDocs"
        //retention... policy.. index

        override val onConnection: (MongoCollection<BsonDocument>) -> Unit = { coll ->
            coll.createIndex(
                Indexes.ascending("int", "string"),
                IndexOptions().background(true).name("MyIndex")
            )
        }
    }

    object complexDocTable : TypedTable<SealedClass>(JSealedClass) {
        override val collectionName: String = "complexDocs"
    }

    private val provider = MongoProvider(
        MongoConnection(
            connString = "mongodb://localhost:27017",
            timeout = Duration.ofMillis(10)
        ),
        databaseName = "mongoCollTest"
    )

    @Test
    fun `add and retrieve single doc`() {

        val myDoc = SmallClass("abc", 123, 3.14, true)

        val doc = mongoOperation {
            simpleDocTable.drop()
            simpleDocTable.addDocument(myDoc)

            val docs = simpleDocTable.all()
            expectThat(1).isEqualTo(docs.count())
            docs.first()
        }.runOn(provider).expectSuccess()

        expectThat(myDoc).isEqualTo(doc)
    }

    val cleanUp = mongoOperation {
        simpleDocTable.drop()
        complexDocTable.drop()
    }

    val myDocs = (1..100).map { buildSealedClass(it) }
    val write100Doc = mongoOperation {
        complexDocTable.addDocuments(myDocs)
        complexDocTable.countDocuments()
    }
    val readAll = mongoOperation {
        complexDocTable.all()
    }


    @Test
    fun `add and retrieve many random docs`() {

        val tot = cleanUp.bind {
            write100Doc
        }.runOn(provider).expectSuccess()

        expectThat(100L).isEqualTo(tot)

        val allDocs = readAll.runOn(provider).expectSuccess()

        expectThat(allDocs.toList()).isEqualTo(myDocs)
    }

    fun delete3Docs(id: Int) = mongoOperation {
        complexDocTable.removeDocuments("""{ string: "SmallClass$id" }""")
            .expectedOne()
        complexDocTable.removeDocuments("""{ "small_class.string" : "Nested${id + 1}" }""")
            .expectedOne()
        complexDocTable.removeDocuments("""{ "name" : "ClassWithArray${id + 2}" }""")
            .expectedOne()
    }

    private fun Long.expectedOne() =
        let { expectThat(it).isEqualTo(1) }


    @Test
    fun `add and delete`() {

        val tot = cleanUp
            .bind { write100Doc }
            .bind { delete3Docs(42) }
            .bind { readAll }
            .transform { it.count() }
            .runOn(provider).expectSuccess()

        expectThat(97).isEqualTo(tot)
    }

    @Test
    fun `verify Indexes`() {

        val indexes = cleanUp.bindOperation {
            expectThat(complexDocTable.listIndexes().count()).isEqualTo(0)
            simpleDocTable.listIndexes()
        }.runOn(provider).expectSuccess()

        val definitions = indexes.toList().map { it.toJson() }
        println(definitions)
        expectThat(definitions.count()).isEqualTo(2)
        expectThat(definitions[1]).contains("MyIndex")
    }

    @Test
    fun `query and aggregate results`() {

        val aggr = cleanUp.bind {
            write100Doc
        }.bindOperation {
            complexDocTable.aggregate(
                Aggregates.match(Filters.exists("name")),
                Aggregates.group("name", Accumulators.sum("count", 1))
            )
        }.runOn(provider).expectSuccess()

        val count = aggr.single()["count"]!!.asInt32().value
        expectThat(count).isEqualTo(33)

    }

}