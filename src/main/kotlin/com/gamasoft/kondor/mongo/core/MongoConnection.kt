package com.gamasoft.kondor.mongo.core

import java.time.Duration

data class MongoConnection(val connString: String, val timeout: Duration = Duration.ofMillis(100))