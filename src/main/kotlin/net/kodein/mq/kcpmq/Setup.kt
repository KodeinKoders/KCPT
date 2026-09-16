@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.PreparedStatement
import kotlin.use


internal object Setup {
    private class MigrationStatement(
        val sql: MQTables.() -> String,
        val conf: MQTables.(PreparedStatement) -> Unit = {}
    )

    context(tables: MQTables)
    private suspend fun initializePropertiesTable(connection: Connection) = withContext(Dispatchers.IO) {
        connection.prepareStatement("CREATE TABLE IF NOT EXISTS ${tables.properties} (key VARCHAR(32) PRIMARY KEY, value TEXT)").use {
            it.execute()
        }
    }

    private val mqMigrations: List<List<MigrationStatement>> = listOf(
        // 0 -> 1
        listOf(
            MigrationStatement(
                sql = {
                    // language="PostgreSQL"
                    """
                        CREATE TABLE $messages (
                            queue VARCHAR(32) NOT NULL,
                            id UUID NOT NULL,
                            due_at TIMESTAMPTZ NOT NULL,
                            started_at TIMESTAMPTZ DEFAULT NULL,
                            payload JSON NOT NULL,
                            notes JSON DEFAULT '[]',
                            PRIMARY KEY(queue, id)
                        )
                    """
                },
            ),
            MigrationStatement(
                sql =  {
                    // language="PostgreSQL"
                    """
                        CREATE INDEX
                        ON $messages (queue, due_at)
                        WHERE started_at IS NULL
                    """
                },
            )
        )
    )

    context(tables: MQTables)
    internal suspend fun initializeMQSchema(source: Source): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            initializePropertiesTable(connection)
            val targetVersion = mqMigrations.size
            var currentVersion = connection.getProperty(Property.SchemaVersion) ?: 0
            if (currentVersion == targetVersion) return@withContext

            while (currentVersion < targetVersion) {
                connection.transaction {
                    mqMigrations[currentVersion].forEach { statement ->
                        prepareStatement(statement.sql(tables).trimIndent()).use {
                            statement.conf(tables, it)
                            it.execute()
                        }
                    }
                    ++currentVersion
                    setProperty(Property.SchemaVersion, currentVersion)
                }
            }
        }
    }
}
