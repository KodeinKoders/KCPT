package net.kodein.mq.kcpmq

import kotlinx.serialization.json.Json


internal val mqJson = Json {
    ignoreUnknownKeys = true
}
