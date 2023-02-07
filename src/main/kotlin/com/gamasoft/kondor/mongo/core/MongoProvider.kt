package com.gamasoft.kondor.mongo.core

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.SocketSettings
import com.ubertob.kondor.outcome.Outcome
import com.ubertob.kondor.outcome.asFailure
import com.ubertob.kondor.outcome.asSuccess
import java.util.concurrent.TimeUnit

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