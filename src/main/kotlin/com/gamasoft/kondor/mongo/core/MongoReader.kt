package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.outcome.Outcome



typealias MongoReader<T> = ContextReader<MongoSession, T>
typealias MongoOutcome<T> = Outcome<MongoError, T>

fun <T> mongoOperation(operation: MongoSession.() -> T) = MongoReader(operation)

infix fun <T : Any> MongoReader<T>.runOn(provider: MongoProvider): MongoOutcome<T> = provider.tryRun(this)

