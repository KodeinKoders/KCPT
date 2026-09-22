package net.kodein.kcpt

// The one table every sink shares, regardless of which feature it otherwise owns. A single
// MQTables-shaped holder for every table name used to live here instead — but that made this
// module's own type describe both message-queue and cron tables, which is exactly the coupling
// splitting into modules was meant to remove. kcpt-message-queue and kcpt-cron each declare their
// own table holder (MessageQueueTables, CronTables) implementing just this.
@InternalKcptApi
public interface HasPropertiesTable {
    public val properties: String
}

// Table names are interpolated into statement text as identifiers, which no escaping function can
// make safe after the fact: escapeLiteral escapes the body of a '...' literal, and
// escapeIdentifier would quote `public.kcpt_messages` into a single identifier in the current
// schema, breaking the qualification. So they are validated instead, and anything that is not a
// plain optionally-qualified identifier is refused outright.
private val qualifiedIdentifier = Regex("""[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?""")

@InternalKcptApi
public fun validatedTableName(name: String): String {
    require(qualifiedIdentifier.matches(name)) {
        "'$name' is not a valid table name: expected an identifier, optionally qualified " +
            "by a schema, made of letters, digits, underscores and dollar signs, and not " +
            "starting with a digit"
    }
    return name
}
