package com.gamasoft.kondor.mongo.json

import com.gamasoft.kondor.mongo.core.MongoConnection
import com.gamasoft.kondor.mongo.core.TypedTable
import com.gamasoft.kondor.mongo.core.mongoAction
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class AuditsTableTest {

    private object AuditsTable : TypedTable<AuditMessage>(JAuditMessage) {
        override val collectionName: String = "Audits"
        //retention... policy.. index
    }


    private fun buildRandomAudit(i: Int): AuditMessage =
        TODO()

    private val audit = buildRandomAudit(0)

    val oneDocReader = mongoAction {
        AuditsTable.drop()
        AuditsTable.addDocument(audit)
        val docs = AuditsTable.all()
        expectThat(1).isEqualTo(docs.count())
        docs.first()
    }

    val docQueryReader = mongoAction {
        (1..100).forEach {
            AuditsTable.addDocument(buildRandomAudit(it))
        }
        AuditsTable.find("{ index: 42 }").first()
    }


    private val mongoConnection = MongoConnection("mongodb://localhost:27017")

    private val dbName = "MongoProvJsonConvTest"

//    @Test
//    fun `add and query doc safely`() {
//        val provider = MongoProvider(mongoConnection, dbName)
//
//        val myDoc = provider.tryRun(oneDocReader).expectSuccess()
//        expectThat(audit).isEqualTo(myDoc)
//
//    }
//
//
//    @Test
//    fun `parsing query safely`() {
//        val provider = MongoProvider(mongoConnection, dbName)
//
//        val myDoc = provider.tryRun(docQueryReader).expectSuccess()
//        expectThat(42).isEqualTo(myDoc)
//
//    }
}

