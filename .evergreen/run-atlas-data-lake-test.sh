#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

JDK=${JDK:-jdk11}

############################################
#            Main Program                  #
############################################

echo "Running Atlas Data Lake driver tests"

export JAVA_HOME="/opt/java/${JDK}"

# show test output
set -x

echo "Running Atlas Data Lake tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=${JAVA_HOME} --stacktrace --debug --info driver-sync:test --tests AtlasDataLakeTest
