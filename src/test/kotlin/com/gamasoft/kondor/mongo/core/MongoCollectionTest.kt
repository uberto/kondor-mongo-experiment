package com.gamasoft.kondor.mongo.core

import java.time.Duration

class MongoCollectionTest {

    object myCollection : MongoDocCollection() { //JConverter coll
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

//    @Test
//    fun `add and query doc safely`() {
//
//        val doc = mongoOperation {
//            myCollection.drop()
//            myCollection.addDocument(doc)
//
//
//            val docs = myCollection.all()
//            Assertions.assertEquals(1, docs.count())
//            docs.first()
//        } runOn provider
//
////        doc.expectSuccess()
////        val myDoc = provider.tryRun(oneDocReader).orThrow()
//        Assertions.assertEquals(doc, myDoc)
//    }
}