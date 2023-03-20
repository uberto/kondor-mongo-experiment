package com.gamasoft.kondor.mongo.json

import com.gamasoft.kondor.mongo.core.MongoConnection
import com.gamasoft.kondor.mongo.core.MongoProvider
import com.gamasoft.kondor.mongo.core.TypedTable
import com.gamasoft.kondor.mongo.core.mongoAction
import com.ubertob.kondortools.expectSuccess
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.time.LocalDate

class FlatDocTableTest {

    private object FlatDocs: TypedTable<SimpleFlatDoc>(JSimpleFlatDoc) {
        override val collectionName: String = "FlatDocs"
        //retention... policy.. index
    }



    private fun createDoc(i: Int): SimpleFlatDoc =
        SimpleFlatDoc(
            index = i,
            name = "mydoc $i",
            date = LocalDate.now().minusDays(i.toLong()),
            bool = i % 2 == 0
        )

    private val doc = createDoc(0)

    val oneDocReader = mongoAction {
        FlatDocs.drop()
        FlatDocs.addDocument(doc)
        val docs = FlatDocs.all()
        expectThat(1).isEqualTo( docs.count())
        docs.first()
    }

    val docQueryReader = mongoAction {
        (1..100).forEach {
            FlatDocs.addDocument(createDoc(it))
        }
        FlatDocs.find("{ index: 42 }").first()
    }





    private val mongoConnection = MongoConnection("mongodb://localhost:27017")

    private val dbName = "MongoProvJsonConvTest"

    @Test
    fun `add and query doc safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val myDoc = provider(oneDocReader).expectSuccess()
        expectThat(doc).isEqualTo( myDoc)

    }


    @Test
    fun `parsing query safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val myDoc = provider(docQueryReader).expectSuccess()
        expectThat(42).isEqualTo( myDoc.index)

    }
}


