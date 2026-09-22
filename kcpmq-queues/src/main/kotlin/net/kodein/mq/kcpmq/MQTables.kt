package net.kodein.mq.kcpmq


@ConsistentCopyVisibility
internal data class MQTables private constructor(
    val properties: String,
    val messages: String,
    val crons: String,
    val cronTicks: String,
) {
    companion object {
        // Table names are interpolated into statement text as identifiers, which no escaping
        // function can make safe after the fact: escapeLiteral escapes the body of a '...' literal,
        // and escapeIdentifier would quote `public.mq_messages` into a single identifier in the
        // current schema, breaking the qualification. So they are validated instead, and anything
        // that is not a plain optionally-qualified identifier is refused outright.
        private val qualifiedIdentifier = Regex("""[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?""")

        private fun validated(name: String): String {
            require(qualifiedIdentifier.matches(name)) {
                "'$name' is not a valid table name: expected an identifier, optionally qualified " +
                    "by a schema, made of letters, digits, underscores and dollar signs, and not " +
                    "starting with a digit"
            }
            return name
        }

        fun named(
            properties: String,
            messages: String,
            crons: String,
            cronTicks: String,
        ) = MQTables(
            properties = validated(properties),
            messages = validated(messages),
            crons = validated(crons),
            cronTicks = validated(cronTicks),
        )
    }
}
