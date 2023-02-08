package com.gamasoft.kondor.mongo.core

import com.ubertob.kondor.json.ObjectNodeConverter
import org.bson.BsonDocument

interface MongoRepo<T : Any> { //actual collections are objects
    val collectionName: String
    //todo retention policy etc.


    fun fromBsonDoc(doc: BsonDocument): T
    fun toBsonDoc(obj: T): BsonDocument

}

abstract class BsonRepo : MongoRepo<BsonDocument> {
    override fun fromBsonDoc(doc: BsonDocument): BsonDocument = doc
    override fun toBsonDoc(obj: BsonDocument): BsonDocument = obj

}

abstract class KondorRepo<T : Any>(private val converter: ObjectNodeConverter<T>) : MongoRepo<T> {
    override fun fromBsonDoc(doc: BsonDocument): T = converter.fromJson(doc.toJson()).orThrow()
    override fun toBsonDoc(obj: T): BsonDocument = BsonDocument.parse(converter.toJson(obj))
}
