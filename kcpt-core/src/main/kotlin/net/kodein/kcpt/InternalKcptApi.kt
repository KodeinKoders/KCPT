package net.kodein.kcpt

// Some declarations have to be public to cross the kcpt-core / kcpt-message-queue / kcpt-cron
// module boundary — Kotlin's `internal` is module-scoped, so it cannot express "internal to this
// library, across several of its own modules" on its own. This marker restores that distinction:
// gated declarations are real public API in the sense that they compile from another module, but
// an outside consumer opting into them is refused with the message below, naming exactly what it
// is opting into and why. There is no compatibility guarantee across releases for anything behind
// it — unlike everything else these modules publish, which is documented, stable API.
@RequiresOptIn(
    level = RequiresOptIn.Level.ERROR,
    message = "Internal to KCPT, shared only across its own modules; no compatibility guarantee across releases.",
)
@Retention(AnnotationRetention.BINARY)
public annotation class InternalKcptApi
