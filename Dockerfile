FROM gradle:6.6.1-jdk11 as builder

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
    && cp ./docker-entrypoint.sh /

# Attempt the ol' multi-stage build to hide your build ARGS trick until DockerHub supports secrets
FROM gradle:6.6.1-jdk11 as final

COPY --from=builder /home/gradle/alarm-state-processor/build/install* /opt
COPY --from=builder /home/gradle/alarm-state-processor/docker-entrypoint.sh /

WORKDIR /opt/alarm-state-processor

ENTRYPOINT ["/docker-entrypoint.sh"]