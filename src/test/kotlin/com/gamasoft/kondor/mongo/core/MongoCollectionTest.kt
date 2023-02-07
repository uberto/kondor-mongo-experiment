package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.json.JAny
import com.ubertob.kondor.json.bool
import com.ubertob.kondor.json.jsonnode.JsonNodeObject
import com.ubertob.kondor.json.num
import com.ubertob.kondor.json.str
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class MongoCollectionTest {

    data class SmallClass(val string: String, val int: Int, val double: Double, val boolean: Boolean)

    object JSmallClass: JAny<SmallClass>(){
        val string by str(SmallClass::string)
        val int by num(SmallClass::int)
        val double by num(SmallClass::double)
        val boolean by bool(SmallClass::boolean)

        override fun JsonNodeObject.deserializeOrThrow() =
            SmallClass(
                string = +string,
                int = +int,
                double = +double,
                boolean = +boolean
            )
    }

    object smallClassCollection : KondorCollection<SmallClass>() {
        override val converter = JSmallClass
        override val collectionName: String = "myCollection"
        //retention... policy.. index
    }

    private val provider = MongoProvider(
        MongoConnection(
            connString = "mongodb://localhost:27017",
            timeout = Duration.ofMillis(10)
        ),
        "mongoCollTest"
    )

    @Test
    fun `add and query doc safely`() {

        val myDoc = SmallClass("abc", 123, 3.14, true)

        val doc = mongoOperation {
            smallClassCollection.drop()
            smallClassCollection.addDocument(myDoc)

            val docs = smallClassCollection.all()
            assertEquals(1, docs.count())
            docs.first()
        }.runOn(provider).orThrow()

        assertEquals(myDoc, doc)
    }
}