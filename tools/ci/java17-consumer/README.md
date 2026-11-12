# Java 17 published consumer check

The `Java 17 consumer` workflow publishes only the root and JVM variants of core and transport to a private file repository. A copied standalone consumer resolves those exact fixture coordinates and Maven Central dependencies, with no composite build or Maven local repository. The check inspects Gradle JVM metadata and every class in both JVM JARs, runs the consumer on Java 17, and runs the projects' JVM tests with a Java 17 launcher.

To reproduce on an isolated runner with JDKs 17 and 21 installed, set `RUNNER_TEMP`, `JAVA_HOME_17_X64`, `GITHUB_RUN_ID`, and `GITHUB_RUN_ATTEMPT`, make JDK 21 the Gradle daemon's `JAVA_HOME`, then run from the repository root:

```sh
tools/ci/java17-consumer.sh
```

Use a fresh run ID and private empty runner temp directory for each run. The workflow uses the repository's checksum-verified Gradle wrapper and never publishes to a remote repository.
