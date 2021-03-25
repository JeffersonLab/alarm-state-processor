FROM gradle:6.6.1-jdk11 as builder

# Build uses GitHub Packages, which unfortunately REQUIRES authentication, even to public repo
ARG GITHUB_USERNAME
ARG GITHUB_TOKEN
ARG CUSTOM_CRT_URL

RUN git clone https://github.com/JeffersonLab/alarm-state-processor \
    && cd ./alarm-state-processor \
    && if [ -z "$CUSTOM_CRT_URL" ] ; then echo "No custom cert needed"; else \
        wget -O /usr/local/share/ca-certificates/customcert.crt $CUSTOM_CRT_URL \
        && update-ca-certificates \
        && keytool -import -alias custom -file /usr/local/share/ca-certificates/customcert.crt -cacerts -storepass changeit -noprompt \
        && export OPTIONAL_CERT_ARG=-Djavax.net.ssl.trustStore=$JAVA_HOME/lib/security/cacerts \
        ; fi \
    && gradle build $OPTIONAL_CERT_ARG \
    && cp -r ./build/install/* /opt \
    && cp ./docker-entrypoint.sh / \
    && rm -rf /home/gradle/alarm-state-processor

WORKDIR /opt/alarm-state-processor/bin

ENTRYPOINT ["/docker-entrypoint.sh"]