package net.kodein.mq.kcpmq

import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import kotlin.test.BeforeTest


abstract class AbstractPgsqlTest {

    protected val pgContainer = PostgreSQLContainer("postgres:18.6-alpine")

    protected lateinit var dataSource: HikariDataSource

    @BeforeTest
    fun setup() {
        pgContainer.start()

        dataSource = HikariDataSource().apply {
            jdbcUrl = pgContainer.jdbcUrl
            username = pgContainer.username
            password = pgContainer.password
        }
    }

    fun shutdown() {
        dataSource.close()
        pgContainer.stop()
    }

}
