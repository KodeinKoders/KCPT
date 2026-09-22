package net.kodein.kcpt

import kotlinx.serialization.json.Json


@InternalKcptApi
public val kcptJson: Json = Json {
    ignoreUnknownKeys = true
}
