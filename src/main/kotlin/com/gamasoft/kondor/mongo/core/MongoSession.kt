package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.BsonObjectId
import java.util.concurrent.atomic.AtomicReference

interface MongoSession {

    //Edit Methods
    fun <T : Any> MongoTable<T>.addDocument(doc: T): BsonObjectId =
        addDocuments(listOf(doc)).first()

    fun <T : Any> MongoTable<T>.all(): Sequence<T> = find("")

    //    fun <T : Any> MongoCollection<T>.replaceDocument(doc: BsonDocument): BsonObjectId
    fun <T : Any> MongoTable<T>.addDocuments(docs: List<T>): Collection<BsonObjectId>
    fun <T : Any> MongoTable<T>.removeDocuments(queryString: String): Long


    //Query Methods
    fun <T : Any> MongoTable<T>.find(queryString: String): Sequence<T>

    fun MongoTable<*>.countDocuments(): Long

    //Other Methods
    fun <T : Any> MongoTable<T>.drop()

    fun <T : Any> MongoTable<T>.listIndexes(): Sequence<BsonDocument>

}


class MongoDbSession(val database: MongoDatabase) : MongoSession {

    val collectionCache = AtomicReference<Map<String, MongoCollection<BsonDocument>>>(emptyMap())

   private fun withCollection(mongoTable: MongoTable<*>): MongoCollection<BsonDocument> =
       collectionCache.get().getOrElse(mongoTable.collectionName) {
           database.getCollection(
               mongoTable.collectionName, BsonDocument::class.java
           ).also { mongoTable.onConnection(it) }
       }



    fun <T : Any> MongoTable<*>.internalRun(block: (MongoCollection<BsonDocument>) -> T): T =
        block(withCollection(this)

        )

    override fun <T : Any> MongoTable<T>.addDocuments(docs: List<T>): Collection<BsonObjectId> =
        internalRun {
            val values = it.insertMany(docs.map(this::toBsonDoc))
                .insertedIds.values
            values.map { objId -> objId as BsonObjectId }
        }

    override fun <T : Any> MongoTable<T>.removeDocuments(queryString: String): Long =
        internalRun {
            it.deleteMany( BsonDocument.parse(queryString))
                .deletedCount
        }

    override fun <T : Any> MongoTable<T>.find(queryString: String): Sequence<T> =
        internalRun {
            when (queryString) {
                "" -> it.find()
                else ->
                    it.find(BsonDocument.parse(queryString))
            }.asSequence().map { this.fromBsonDoc(it) }
        }

    override fun MongoTable<*>.countDocuments(): Long =
        internalRun {
            it.countDocuments()
        }
    override fun <T : Any> MongoTable<T>.drop() =
        internalRun {
            it.drop()
        }

    override fun <T : Any> MongoTable<T>.listIndexes(): Sequence<BsonDocument> =
        internalRun {
            it.listIndexes().map { it.toBsonDocument() }.iterator().asSequence()
        }
}
