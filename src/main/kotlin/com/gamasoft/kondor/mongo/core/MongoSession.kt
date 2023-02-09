package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.BsonDocument
import org.bson.BsonObjectId

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
}


class MongoDbSession(val database: MongoDatabase) : MongoSession {
    fun <T : Any> MongoTable<*>.internalRun(block: (MongoCollection<BsonDocument>) -> T): T =
        block(
            database.getCollection(
                collectionName, BsonDocument::class.java
            )
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


}
