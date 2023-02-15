package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.outcome.Outcome


typealias MongoReader<T> = ContextReader<MongoSession, T>
typealias MongoOutcome<T> = Outcome<MongoError, T>

fun <U, T> mongoCalculation(operation: MongoSession.(U) -> T): (U) -> ContextReader<MongoSession, T> =
    { input: U -> MongoReader { session -> operation(session, input) } }

fun <T> mongoAction(operation: MongoSession.() -> T): ContextReader<MongoSession, T> =
    mongoAction(operation)

fun <T, U> MongoReader<T>.bindCalculation(operation: MongoSession.(T) -> U): ContextReader<MongoSession, U> =
    bind { input -> mongoCalculation(operation)(input) }

infix fun <T : Any> MongoReader<T>.runOn(provider: MongoProvider): MongoOutcome<T> = provider.tryRun(this)

