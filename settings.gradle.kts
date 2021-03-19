rootProject.name = "kspark"

include(
    "core"
)

for (project in rootProject.children) {
    project.apply {
        name = "kspark-$name"
    }
}
