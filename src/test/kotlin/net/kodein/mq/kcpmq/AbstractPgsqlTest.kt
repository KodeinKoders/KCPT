package net.kodein.mq.kcpmq

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid


abstract class AbstractPgsqlTest {

    companion object {
        val pgContainer = PostgreSQLContainer("postgres:18.6-alpine").apply { start() }
    }

    protected open val poolSize: Int = 12

    protected lateinit var dataSource: HikariDataSource

    @BeforeTest
    fun setup() {
        DriverManager.getConnection(pgContainer.jdbcUrl, pgContainer.username, pgContainer.password).use { connection ->
            connection.createStatement().use { it.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;") }
        }

        dataSource = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
            maximumPoolSize = poolSize
        }
    }

    @AfterTest
    fun tearDown() {
        dataSource.close()
    }

    protected fun uniqueQueueName(): String = "q_${Uuid.random().toString().replace("-", "").take(16)}"

    protected fun uniquePrefix(): String = "public.p_${Uuid.random().toString().replace("-", "").take(12)}_"

    protected suspend fun awaitUntil(
        timeout: Duration = 5.seconds,
        poll: Duration = 50.milliseconds,
        predicate: suspend () -> Boolean,
    ) {
        val deadline = kotlin.time.Clock.System.now() + timeout
        while (!predicate()) {
            if (kotlin.time.Clock.System.now() >= deadline) {
                throw AssertionError("condition did not become true within $timeout")
            }
            kotlinx.coroutines.delay(poll)
        }
    }
}
