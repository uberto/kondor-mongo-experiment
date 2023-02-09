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

        expectThat(myDoc).isEqualTo( doc)
    }

    val cleanUp = mongoOperation {
        complexDocTable.drop()
    }

    val myDocs = (1..100).map { buildSealedClass(it) }
    val write100Doc = mongoOperation {
        myDocs.forEach {
            complexDocTable.addDocument(it)
        }
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

        expectThat(100L).isEqualTo( tot)

        val allDocs = readAll.runOn(provider).expectSuccess().toList()
        expectThat(myDocs).isEqualTo( allDocs)
    }
}