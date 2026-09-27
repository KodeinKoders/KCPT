package net.kodein.kcpt

// One version step of a feature module's schema, rendered for a given table prefix — the exact
// statements that module runs itself when it manages its own schema. Public so an application
// that manages its schema some other way (Flyway, Liquibase, a DBA-reviewed script) can apply the
// same DDL instead: pure DDL, none of KCPT's own version bookkeeping, since in that mode KCPT
// never reads or writes it.
public class SchemaMigration(
    // The schema version these statements bring the tables to, from toVersion - 1.
    public val toVersion: Int,
    // Each one a single statement, without a trailing semicolon, in the order they must run.
    public val statements: List<String>,
) {
    // Every statement as one script, for tools that take one.
    public val script: String get() = statements.joinToString(";\n\n", postfix = ";\n")
}
