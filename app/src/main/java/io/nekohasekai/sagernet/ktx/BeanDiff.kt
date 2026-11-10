package io.nekohasekai.sagernet.ktx

import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import io.nekohasekai.sagernet.fmt.AbstractBean
import moe.matsuri.nb4a.utils.JavaUtil

private val DIFF_IGNORE_FIELDS = setOf(
    "name",
    "finalAddress",
    "finalPort",
    "customOutboundJson",
    "customConfigJson",
    "serializeWithoutName",
)

private val DIFF_FIELD_LABELS = mapOf(
    "serverAddress" to "server",
    "serverPort" to "port",
    "utlsFingerprint" to "fp",
    "realityPubKey" to "pbk",
    "realityShortId" to "sid",
    "allowInsecure" to "insecure",
    "packetEncoding" to "packet",
    "wsMaxEarlyData" to "ws-ed",
    "earlyDataHeaderName" to "ws-eh",
    "enableECH" to "ech",
    "echConfig" to "ech-config",
    "enableMux" to "mux",
    "muxPadding" to "mux-padding",
    "muxType" to "mux-type",
    "muxConcurrency" to "mux-concurrency",
)

private const val DIFF_VALUE_LIMIT = 64

/**
 * Human-readable field differences between two beans.
 * Intended for subscription update dialogs.
 */
fun AbstractBean.diffFields(other: AbstractBean): List<String> {
    val left = beanDiffObject(this)
    val right = beanDiffObject(other)
    val keys = (left.keySet() + right.keySet())
        .filter { it !in DIFF_IGNORE_FIELDS }
        .sorted()

    val result = ArrayList<String>()
    for (key in keys) {
        val oldValue = formatDiffValue(left.get(key))
        val newValue = formatDiffValue(right.get(key))
        if (oldValue == newValue) continue
        result.add("${DIFF_FIELD_LABELS[key] ?: key}: $oldValue → $newValue")
    }
    return result
}

fun formatSubscriptionUpdated(updated: Map<String, List<String>>): String {
    if (updated.isEmpty()) return ""

    val singleFieldChanges = ArrayList<Triple<String, String, String>>()
    var canCollapse = true
    for ((name, diffs) in updated) {
        val diff = diffs.singleOrNull()
        if (diff == null) {
            canCollapse = false
            break
        }
        val sep = diff.indexOf(':')
        if (sep <= 0) {
            canCollapse = false
            break
        }
        val field = diff.substring(0, sep).trim()
        val change = diff.substring(sep + 1).trim()
        val arrow = change.indexOf('→')
        if (arrow < 0) {
            canCollapse = false
            break
        }
        singleFieldChanges.add(Triple(name, field, change))
    }

    if (canCollapse && singleFieldChanges.isNotEmpty()) {
        val fields = singleFieldChanges.map { it.second }.toSet()
        if (fields.size == 1) {
            val field = fields.first()
            val body = singleFieldChanges.joinToString("\n") { (name, _, change) ->
                "• $name: $change"
            }
            return "$field (${updated.size}):\n$body"
        }
    }

    return updated.entries.joinToString("\n\n") { (name, diffs) ->
        if (diffs.isEmpty()) {
            "• $name"
        } else {
            buildString {
                append("• ").append(name)
                for (diff in diffs) {
                    append("\n    ").append(diff)
                }
            }
        }
    }
}

private fun beanDiffObject(bean: AbstractBean): JsonObject {
    val tree = JavaUtil.gson.toJsonTree(bean)
    return if (tree.isJsonObject) tree.asJsonObject else JsonObject()
}

private fun formatDiffValue(element: JsonElement?): String {
    if (element == null || element is JsonNull) return "∅"
    if (element is JsonPrimitive) {
        return truncateDiffValue(element.asString)
    }
    return truncateDiffValue(element.toString())
}

private fun truncateDiffValue(value: String): String {
    val compact = value.replace('\n', ' ').trim()
    if (compact.isEmpty()) return "∅"
    return if (compact.length <= DIFF_VALUE_LIMIT) {
        compact
    } else {
        compact.take(DIFF_VALUE_LIMIT - 1) + "…"
    }
}
