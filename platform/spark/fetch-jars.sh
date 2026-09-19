#!/bin/sh
# Downloads the Hadoop S3A connector jars Spark needs to read and write MinIO/S3.
# Runs once as the `spark-jars` compose service; files persist in the `spark-jars` volume.
# Versions must match the Hadoop build bundled with the Spark image (Spark 4.1.3 -> Hadoop 3.4.2).
set -eu

TARGET=/jars
MAVEN=https://repo1.maven.org/maven2

fetch() {
  path="$1"
  file="$TARGET/$(basename "$path")"
  if [ -f "$file" ]; then
    echo "present: $(basename "$file")"
    return
  fi
  echo "downloading: $(basename "$file")"
  curl -fsSL --retry 5 -o "$file.part" "$MAVEN/$path"
  expected="$(curl -fsSL --retry 5 "$MAVEN/$path.sha1" | cut -c1-40)"
  actual="$(sha1sum "$file.part" | cut -c1-40)"
  if [ "$expected" != "$actual" ]; then
    echo "checksum mismatch for $file (expected $expected, got $actual)" >&2
    rm -f "$file.part"
    exit 1
  fi
  mv "$file.part" "$file"
}

mkdir -p "$TARGET"
fetch org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
fetch software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar
fetch software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.2.1/analyticsaccelerator-s3-1.2.1.jar
echo "spark jars ready in $TARGET"
