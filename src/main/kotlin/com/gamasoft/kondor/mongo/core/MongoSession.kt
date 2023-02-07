package com.gamasoft.kondor.mongo.core

import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.`in`
import org.bson.BsonDocument
import org.bson.BsonObjectId
import java.util.Collection

interface MongoSession {
    fun  <T : Any> MongoCollection<T>.addDocument(doc: T): BsonObjectId =
        addDocuments(listOf(doc)).first()

    fun <T : Any> MongoCollection<T>.removeDocument(id: BsonObjectId): Boolean =
        removeDocuments(listOf(id)) > 0

    fun <T : Any> MongoCollection<T>.all(): Sequence<T> = find("")

    //    fun <T : Any> MongoCollection<T>.replaceDocument(doc: BsonDocument): BsonObjectId
    fun <T : Any> MongoCollection<T>.addDocuments(docs: List<T>): Collection<BsonObjectId>
    fun <T : Any> MongoCollection<T>.removeDocuments(ids: List<BsonObjectId>): Long
    fun <T : Any> MongoCollection<T>.find(queryString: String): Sequence<T>
    fun <T : Any> MongoCollection<T>.drop()
}


class MongoDbSession(val database: MongoDatabase) : MongoSession {
    private fun <T:Any> MongoCollection<T>.driverCollection() = database.getCollection(collectionName, BsonDocument::class.java)
    override fun <T : Any> MongoCollection<T>.addDocuments(docs: List<T>): Collection<BsonObjectId> =
        driverCollection()
            .insertMany(docs.map(this::toBsonDoc))
            .insertedIds.values as Collection<BsonObjectId>
    override fun <T : Any> MongoCollection<T>.removeDocuments(ids: List<BsonObjectId>): Long =
        driverCollection().deleteMany(  `in`("id", ids.toTypedArray() ))
            .deletedCount
    override fun <T : Any> MongoCollection<T>.find(queryString: String): Sequence<T> =
            when (queryString) {
            "" -> driverCollection().find()
            else -> BsonDocument.parse(queryString).let { filter ->
                driverCollection().find(filter)
            }
        }.asSequence().map { this.fromBsonDoc(it) }

    override fun <T : Any> MongoCollection<T>.drop() =driverCollection().drop()


}
