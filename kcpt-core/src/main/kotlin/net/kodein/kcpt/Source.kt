package net.kodein.kcpt

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import javax.sql.DataSource


public typealias Source = suspend () -> Connection

public fun DataSource.asSource(): Source = { withContext(Dispatchers.IO) { connection } }
