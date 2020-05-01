#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       JDK                     Set the version of java to be used.  Java versions can be set from the java toolchain /opt/java
#                               "jdk5", "jdk6", "jdk7", "jdk8", "jdk9", "jdk11"

JDK=${JDK:-jdk11}
OCSP_MUST_STAPLE=${OCSP_MUST_STAPLE:-}

############################################
#            Functions                     #
############################################

provision_ssl () {
  echo "SSL !"

  cp ${JAVA_HOME}/lib/security/cacerts mongo-truststore
  ${JAVA_HOME}/bin/keytool -import -trustcacerts -file ${CA_FILE} -keystore mongo-truststore -alias ca_ocsp -storepass changeit -noprompt

  # We add extra gradle arguments for SSL
  export GRADLE_EXTRA_VARS="-Pssl.enabled=true -Pocsp.property=`pwd`/java-security-ocsp-property -Pssl.trustStoreType=jks -Pssl.trustStore=`pwd`/mongo-truststore -Pssl.trustStorePassword=changeit -Pssl.checkRevocation=true -Pclient.enableStatusRequestExtension=${OCSP_MUST_STAPLE} -Pclient.protocols=TLSv1.2"
}

############################################
#            Main Program                  #
############################################

echo "Running OCSP tests"

export JAVA_HOME="/opt/java/${JDK}"
export OCSP_TLS_SHOULD_SUCCEED=${OCSP_TLS_SHOULD_SUCCEED}

# show test output
set -x

provision_ssl

echo "Running OCSP tests with ${JDK}"
./gradlew -version
./gradlew -PjdkHome=${JAVA_HOME} ${GRADLE_EXTRA_VARS} --stacktrace --debug --info driver-sync:test --tests OcspTest

