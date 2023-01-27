package com.gamasoft.kondor.mongo.json

import com.gamasoft.kondor.mongo.core.MongoConnection
import com.gamasoft.kondor.mongo.core.MongoProvider
import com.gamasoft.kondor.mongo.core.collForTest
import com.gamasoft.kondor.mongo.core.runOnMongo
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

    data class MyDoc4Mongo(val index: Int, val name: String, val date: LocalDate, val bool: Boolean)

    object JMyDoc4Mongo: JAny<MyDoc4Mongo>() {

        val index by num(MyDoc4Mongo::index)
        val name by str(MyDoc4Mongo::name)
        val localDate by str(MyDoc4Mongo::date)
        val yesOrNo by bool(MyDoc4Mongo::bool)

        override fun JsonNodeObject.deserializeOrThrow() = MyDoc4Mongo(
            index = +index,
            name = +name,
            date = +localDate,
            bool = +yesOrNo
        )
    }

    fun createDoc(index: Int): BsonDocument {
        val obj = buildMyDoc4Mongo(index)
        val json = JMyDoc4Mongo.toJson(obj)
        println("!!! $json")
        val bsonDocument = BsonDocument.parse(json)
        println(">>> ${bsonDocument.toJson()}")
        return bsonDocument
    }

    private fun buildMyDoc4Mongo(i: Int): MyDoc4Mongo =
        MyDoc4Mongo(
            index = i,
            name = "mydoc $i",
            date = LocalDate.now().minusDays(i.toLong()),
            bool = i % 2 == 0
        )

    private val doc = createDoc(0)

    val oneDocReader = runOnMongo {
        collForTest.drop()
        collForTest.addDocument(doc)
        val docs = collForTest.all()
        assertEquals(1, docs.count())
        docs.first()
    }

    val docQueryReader = runOnMongo {
        (1..100).forEach {
            collForTest.addDocument(createDoc(it))
        }
        collForTest.find("{ index: 42 }").first()
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