#!/usr/bin/env bash
set -euo pipefail

: "${RUNNER_TEMP:?This check requires a private runner temp directory}"
: "${JAVA_HOME_17_X64:?JDK 17 must be installed}"
: "${GITHUB_RUN_ID:?A unique run ID is required}"
: "${GITHUB_RUN_ATTEMPT:?A unique run attempt is required}"

export FIXTURE_VERSION="0.0.0-java17-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
export FIXTURE_REPOSITORY="$RUNNER_TEMP/jsonrpc4k-java17-repo-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
consumer_dir="$RUNNER_TEMP/jsonrpc4k-java17-consumer-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
mkdir -p "$FIXTURE_REPOSITORY" "$consumer_dir"
cp -R tools/ci/java17-consumer/. "$consumer_dir/"

./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m \
	-PVERSION_NAME="$FIXTURE_VERSION" \
	--init-script tools/ci/java17-consumer/publish.init.gradle.kts \
	:jsonrpc4k-core:publishKotlinMultiplatformPublicationToFixtureRepository \
	:jsonrpc4k-core:publishJvmPublicationToFixtureRepository \
	:jsonrpc4k-transport:publishKotlinMultiplatformPublicationToFixtureRepository \
	:jsonrpc4k-transport:publishJvmPublicationToFixtureRepository

status=0
python3 - <<'PY' || status=1
import json
import os
from pathlib import Path
import struct
import zipfile

root = Path(os.environ["FIXTURE_REPOSITORY"]) / "io/github/albertocavalcante"
version = os.environ["FIXTURE_VERSION"]
failed = False
for name in ("jsonrpc4k-core-jvm", "jsonrpc4k-transport-jvm"):
    directory = root / name / version
    root_name = name.removesuffix("-jvm")
    for component in (root_name, name):
        metadata = json.loads((root / component / version / f"{component}-{version}.module").read_text())
        variants = [v for v in metadata["variants"] if v["attributes"].get("org.jetbrains.kotlin.platform.type") == "jvm" and v["attributes"].get("org.gradle.usage") in ("java-api", "java-runtime")]
        print(f"{component}: JVM variant attributes {[(v['name'], v['attributes']) for v in variants]}")
        if {v["attributes"].get("org.gradle.usage") for v in variants} != {"java-api", "java-runtime"} or any(v["attributes"].get("org.gradle.jvm.version") != 17 for v in variants):
            print(f"{component}: missing or incompatible JVM API/runtime metadata")
            failed = True
    with zipfile.ZipFile(directory / f"{name}-{version}.jar") as jar:
        classes = [n for n in jar.namelist() if n.endswith(".class")]
        if not classes:
            raise SystemExit(f"{name}: no classes found")
        majors = [struct.unpack(">H", jar.read(n)[6:8])[0] for n in classes]
        print(f"{name}: maximum class major {max(majors)}")
        if max(majors) > 61:
            print(f"{name}: Java 17 cannot load class files")
            failed = True
if failed:
    raise SystemExit(1)
PY

JAVA_HOME="$JAVA_HOME_17_X64" PATH="$JAVA_HOME_17_X64/bin:$PATH" \
	./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m -p "$consumer_dir" run verifyJava16Rejected || status=1
./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m \
	--init-script tools/ci/java17-consumer/publish.init.gradle.kts \
	:jsonrpc4k-core:jvmTest :jsonrpc4k-transport:jvmTest || status=1
exit "$status"
