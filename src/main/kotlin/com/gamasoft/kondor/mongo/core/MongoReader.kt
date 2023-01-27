package com.gamasoft.kondor.mongo.core

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.SocketSettings
import com.ubertob.kondor.outcome.Outcome
import com.ubertob.kondor.outcome.OutcomeError
import com.ubertob.kondor.outcome.asFailure
import com.ubertob.kondor.outcome.asSuccess
import org.bson.BsonDocument
import org.bson.BsonObjectId
import java.time.Duration
import java.util.Collection
import java.util.concurrent.TimeUnit


data class MongoConnection(val connString: String, val timeout: Duration = Duration.ofMillis(100))

interface MongoCollection { //actual collections are objects
    val collectionName: String
    //todo retention policy etc.
}

interface MongoSession {
    fun MongoCollection.addDocument(doc: BsonDocument): BsonObjectId =
        addDocuments(listOf(doc)).first()

    fun MongoCollection.removeDocument(id: BsonObjectId) =
        removeDocuments(listOf(id)).first()

    fun MongoCollection.all(): Sequence<BsonDocument> = find("")

    //    fun MongoCollection.replaceDocument(doc: BsonDocument): BsonObjectId
    fun MongoCollection.addDocuments(docs: List<BsonDocument>): Collection<BsonObjectId>
    fun MongoCollection.removeDocuments(ids: List<BsonObjectId>): List<BsonObjectId>
    fun MongoCollection.find(queryString: String): Sequence<BsonDocument>
    fun MongoCollection.drop()
}

typealias MongoReader<T> = ContextReader<MongoSession, T>

fun <T> runOnMongo(operation: MongoSession.() -> T) = ContextReader<MongoSession, T>(operation)

class MongoProvider(private val connection: MongoConnection, val databaseName: String) : ContextProvider<MongoSession> {
    override fun <T> tryRun(reader: ContextReader<MongoSession, T>): Outcome<MongoError, T> =
        try {
            val mongoClient: MongoClient = MongoClients.create(mongoClientSettings())

            println("Connected to ${connection.connString} found dbs: ${mongoClient.listDatabases()}") //todo remove it later
            val sess = MongoDbSession(mongoClient.getDatabase(databaseName))
            reader.runWith(sess).asSuccess()
        } catch (e: Exception) {
            MongoErrorException(connection, databaseName, e).asFailure()
        }

    private fun mongoClientSettings(): MongoClientSettings = MongoClientSettings.builder()
        .applyToSocketSettings { builder: SocketSettings.Builder ->
            builder.applySettings(
                builder.connectTimeout(connection.timeout.toMillis().toInt(), TimeUnit.MILLISECONDS).build()
            )
        }
        .applyToClusterSettings { builder: ClusterSettings.Builder ->
            builder.serverSelectionTimeout(
                connection.timeout.toMillis(),
                TimeUnit.MILLISECONDS
            ).build()
        }
        .applyConnectionString(ConnectionString(connection.connString))
        .build()

}

sealed class MongoError : OutcomeError

data class MongoErrorInternal(val connection: MongoConnection, val databaseName: String, val errorDesc: String) :
    MongoError() {
    override val msg: String = "$errorDesc - dbname:$databaseName instance:${connection.connString}"
}

data class MongoErrorException(val connection: MongoConnection, val databaseName: String, val e: Exception) :
    MongoError() {
    override val msg: String = "$e - dbname:$databaseName instance:${connection.connString}"
}

class MongoDbSession(val database: MongoDatabase) : MongoSession {

    private fun MongoCollection.coll() = database.getCollection(collectionName, BsonDocument::class.java)
    override fun MongoCollection.addDocuments(docs: List<BsonDocument>): Collection<BsonObjectId> =
        coll().insertMany(docs).insertedIds.values as Collection<BsonObjectId>

    override fun MongoCollection.removeDocuments(ids: List<BsonObjectId>): List<BsonObjectId> =
        TODO("remove documents")
//        coll().deleteMany(eq).insertedIds.values as List<BsonObjectId>

    override fun MongoCollection.find(queryString: String): Sequence<BsonDocument> =
        when (queryString) {
            "" -> coll().find()
            else -> BsonDocument.parse(queryString).let { filter ->
                coll().find(filter)
            }
        }.asSequence()


    override fun MongoCollection.drop() =
        coll().drop()

}
