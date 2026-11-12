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
    metadata = json.loads((directory / f"{name}-{version}.module").read_text())
    print(f"{name}: variant attributes {[(v['name'], v['attributes']) for v in metadata['variants']]}")
    root_name = name.removesuffix("-jvm")
    root_metadata = json.loads((root / root_name / version / f"{root_name}-{version}.module").read_text())
    print(f"{root_name}: JVM variant attributes {[(v['name'], v['attributes']) for v in root_metadata['variants'] if 'jvm' in v['name'].lower()]}")
    versions = [v["attributes"]["org.gradle.jvm.version"] for v in metadata["variants"] if "org.gradle.jvm.version" in v["attributes"]]
    print(f"{name}: Gradle JVM versions {versions}")
    if not versions or any(v > 17 for v in versions):
        print(f"{name}: incompatible Gradle JVM metadata")
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
	./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m -p "$consumer_dir" run || status=1
./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx1536m \
	--init-script tools/ci/java17-consumer/publish.init.gradle.kts \
	:jsonrpc4k-core:jvmTest :jsonrpc4k-transport:jvmTest || status=1
exit "$status"
