package com.gamasoft.kondor.mongo.json

import com.gamasoft.kondor.mongo.core.BsonTable
import com.gamasoft.kondor.mongo.core.MongoConnection
import com.gamasoft.kondor.mongo.core.MongoProvider
import com.gamasoft.kondor.mongo.core.mongoOperation
import com.ubertob.kondor.json.JAny
import com.ubertob.kondor.json.bool
import com.ubertob.kondor.json.datetime.str
import com.ubertob.kondor.json.jsonnode.JsonNodeObject
import com.ubertob.kondor.json.num
import com.ubertob.kondor.json.str
import org.bson.BsonDocument
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate

class JsonConverter4MongoTest {

    private object collForJsonTest: BsonTable() {
        override val collectionName: String = "collForJsonTest"
        //retention... policy.. index
    }


    data class SimpleFlatDoc(val index: Int, val name: String, val date: LocalDate, val bool: Boolean)

    object JSimpleFlatDoc : JAny<SimpleFlatDoc>() {

        val index by num(SimpleFlatDoc::index)
        val name by str(SimpleFlatDoc::name)
        val localDate by str(SimpleFlatDoc::date)
        val yesOrNo by bool(SimpleFlatDoc::bool)

        override fun JsonNodeObject.deserializeOrThrow() = SimpleFlatDoc(
            index = +index,
            name = +name,
            date = +localDate,
            bool = +yesOrNo
        )
    }

    fun createDoc(index: Int): BsonDocument {
        val obj = buildMyDoc4Mongo(index)
        val json = JSimpleFlatDoc.toJson(obj)
//        println("!!! $json")
        val bsonDocument = BsonDocument.parse(json)
//        println(">>> ${bsonDocument.toJson()}")
        return bsonDocument
    }

    private fun buildMyDoc4Mongo(i: Int): SimpleFlatDoc =
        SimpleFlatDoc(
            index = i,
            name = "mydoc $i",
            date = LocalDate.now().minusDays(i.toLong()),
            bool = i % 2 == 0
        )

    private val doc = createDoc(0)

    val oneDocReader = mongoOperation {
        collForJsonTest.drop()
        collForJsonTest.addDocument(doc)
        val docs = collForJsonTest.all()
        assertEquals(1, docs.count())
        docs.first()
    }

    val docQueryReader = mongoOperation {
        (1..100).forEach {
            collForJsonTest.addDocument(createDoc(it))
        }
        collForJsonTest.find("{ index: 42 }").first()
    }

    private val mongoConnection = MongoConnection("mongodb://localhost:27017")

    private val dbName = "MongoProvJsonConvTest"

    @Test
    fun `add and query doc safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val myDoc = provider.tryRun(oneDocReader).orThrow()
        assertEquals(doc, myDoc)

    }


    @Test
    fun `parsing query safely`() {
        val provider = MongoProvider(mongoConnection, dbName)

        val myDoc = provider.tryRun(docQueryReader).orThrow()
        assertEquals(42, myDoc["index"]!!.asInt32().value)

    }
}