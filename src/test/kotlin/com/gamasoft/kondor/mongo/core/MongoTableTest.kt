package com.gamasoft.kondor.mongo.core

import com.ubertob.kondortools.expectSuccess
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.time.Duration

class MongoTableTest {

    object simpleDocTable : TypedTable<SmallClass>(JSmallClass) {
        override val collectionName: String = "simpleDocs"
        //retention... policy.. index
    }

    object complexDocTable : TypedTable<SealedClass>(JSealedClass) {
        override val collectionName: String = "complexDocs"
        //retention... policy.. index
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


}