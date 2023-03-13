package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.outcome.Outcome


typealias MongoReader<T> = ContextReader<MongoSession, T>
typealias MongoOutcome<T> = Outcome<MongoError, T>

fun <U, T> mongoCalculation(operation: MongoSession.(U) -> T): (U) -> MongoReader<T> = //unit
    { input: U -> MongoReader { session -> operation(session, input) } }

fun <T> mongoAction(operation: MongoSession.() -> T): MongoReader<T> =
    mongoCalculation<Unit, T>{ operation(this) }(Unit)

fun <T, U> MongoReader<T>.bindCalculation(operation: MongoSession.(T) -> U): MongoReader<U> =
    bind { input -> mongoCalculation(operation)(input) }

infix fun <T, U> MongoReader<T>.combineWith(operation: (T) -> MongoReader<U>): MongoReader<U> =
    bind { input -> operation(input) }

fun  <T> MongoReader<T>.ignoreResult(): MongoReader<Unit> =
    transform { }

operator fun  <T, U> MongoReader<T>.plus(operation: (T) -> MongoReader<U>): MongoReader<U> =
    combineWith(operation)

operator fun  <U> MongoReader<Unit>.plus(otherReader: MongoReader<U>): MongoReader<U> =
    combineWith { otherReader }

infix fun <T : Any> MongoReader<T>.runOn(provider: MongoProvider): MongoOutcome<T> = provider.tryRun(this)

