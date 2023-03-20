package com.gamasoft.kondor.mongo.json

import com.ubertob.kondor.json.*
import com.ubertob.kondor.json.bool
import com.ubertob.kondor.json.datetime.str
import com.ubertob.kondor.json.jsonnode.JsonNodeObject
import java.time.LocalDate

data class SimpleFlatDoc(val index: Int, val name: String, val date: LocalDate, val bool: Boolean)

object JSimpleFlatDoc : JAny<SimpleFlatDoc>() {

    val index by num(SimpleFlatDoc::index)
    val name by str(SimpleFlatDoc::name)
    val localDate by str(SimpleFlatDoc::date)
    val yesOrNo by bool(SimpleFlatDoc::bool)

    override fun JsonNodeObject.deserializeOrThrow() = SimpleFlatDoc(
        index = +index,
        name = +name,
        date = +localDate,
        bool = +yesOrNo
    )
}


sealed interface AuditMessage

data class StringAudit(val msg: String) : AuditMessage
data class TextAudit(val text: List<String>) : AuditMessage
data class ErrorCodeAudit(val errorCode: Int, val description: String) : AuditMessage
data class MultiAudit(val audits: List<AuditMessage>) : AuditMessage


object JAuditMessage : JSealed<AuditMessage>() {
    private const val ERRCODE = "ERRCODE"
    private const val STR = "STR"
    private const val TEXT = "TEXT"
    private const val MULTI = "MULTI"

    override val discriminatorFieldName = "___type"

    override val subConverters: Map<String, ObjectNodeConverter<out AuditMessage>> =
        mapOf(
            ERRCODE to JErrorCodeAudit,
            STR to JStringAudit,
            TEXT to JTextAudit,
            MULTI to JMultiAudit
        )

    override fun extractTypeName(obj: AuditMessage): String =
        when (obj) {
            is ErrorCodeAudit -> ERRCODE
            is StringAudit -> STR
            is TextAudit -> TEXT
            is MultiAudit -> MULTI
        }

}

object JStringAudit : JAny<StringAudit>() {
    private val msg by str(StringAudit::msg)

    override fun JsonNodeObject.deserializeOrThrow(): StringAudit =
        StringAudit(
            msg = +msg
        )
}

object JTextAudit : JAny<TextAudit>() {
    private val text by array(JString, TextAudit::text)

    override fun JsonNodeObject.deserializeOrThrow(): TextAudit =
        TextAudit(
            text = +text
        )
}

object JErrorCodeAudit : JAny<ErrorCodeAudit>() {
    private val description by str(ErrorCodeAudit::description)

    private val errorCode by num(ErrorCodeAudit::errorCode)

    override fun JsonNodeObject.deserializeOrThrow(): ErrorCodeAudit =
        ErrorCodeAudit(
            description = +description,
            errorCode = +errorCode
        )
}

object JMultiAudit : JAny<MultiAudit>() {
    private val audits by array(JAuditMessage, MultiAudit::audits)

    override fun JsonNodeObject.deserializeOrThrow() =
        MultiAudit(
            audits = +audits
        )
}

