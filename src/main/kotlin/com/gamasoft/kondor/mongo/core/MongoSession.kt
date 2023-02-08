package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.`in`
import org.bson.BsonDocument
import org.bson.BsonObjectId

interface MongoSession {

    //Edit Methods
    fun <T : Any> MongoRepo<T>.addDocument(doc: T): BsonObjectId =
        addDocuments(listOf(doc)).first()

    fun <T : Any> MongoRepo<T>.removeDocument(id: BsonObjectId): Boolean =
        removeDocuments(listOf(id)) > 0

    fun <T : Any> MongoRepo<T>.all(): Sequence<T> = find("")

    //    fun <T : Any> MongoCollection<T>.replaceDocument(doc: BsonDocument): BsonObjectId
    fun <T : Any> MongoRepo<T>.addDocuments(docs: List<T>): Collection<BsonObjectId>
    fun <T : Any> MongoRepo<T>.removeDocuments(ids: List<BsonObjectId>): Long


    //Query Methods
    fun <T : Any> MongoRepo<T>.find(queryString: String): Sequence<T>

    fun MongoRepo<*>.countDocuments(): Long

    //Other Methods
    fun <T : Any> MongoRepo<T>.drop()
}


class MongoDbSession(val database: MongoDatabase) : MongoSession {
    fun <T : Any> MongoRepo<*>.internalRun(block: (MongoCollection<BsonDocument>) -> T): T =
        block(
            database.getCollection(
                collectionName, BsonDocument::class.java
            )
        )

    override fun <T : Any> MongoRepo<T>.addDocuments(docs: List<T>): Collection<BsonObjectId> =
        internalRun {
            val values = it.insertMany(docs.map(this::toBsonDoc))
                .insertedIds.values
            values.map { objId -> objId as BsonObjectId }
        }

    override fun <T : Any> MongoRepo<T>.removeDocuments(ids: List<BsonObjectId>): Long =
        internalRun {
            it.deleteMany(`in`("id", ids.toTypedArray()))
                .deletedCount
        }

    override fun <T : Any> MongoRepo<T>.find(queryString: String): Sequence<T> =
        internalRun {
            when (queryString) {
                "" -> it.find()
                else ->
                    it.find(BsonDocument.parse(queryString))
            }.asSequence().map { this.fromBsonDoc(it) }
        }

    override fun MongoRepo<*>.countDocuments(): Long =
        internalRun {
            it.countDocuments()
        }
    override fun <T : Any> MongoRepo<T>.drop() =
        internalRun {
            it.drop()
        }


}
