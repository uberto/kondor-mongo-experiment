package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.outcome.Outcome


//- let access the underlying mongo collection
//- add test about aggregation functions
//- collection typed with converter


typealias MongoReader<T> = ContextReader<MongoSession, T>
typealias MongoOutcome<T> = Outcome<MongoError, T>

fun <T> mongoOperation(operation: MongoSession.() -> T) = MongoReader<T>(operation)

infix fun <T : Any> MongoReader<T>.runOn(provider: MongoProvider): MongoOutcome<T> = provider.tryRun(this)

