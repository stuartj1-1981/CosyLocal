ARG BUILD_FROM
FROM ${BUILD_FROM}

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages \
    paho-mqtt==1.6.1

# Copy application and S6 service definitions
COPY rootfs /

# Set permissions on S6 scripts
RUN chmod a+x /etc/services.d/qsh-sniffer/run \
    && chmod a+x /etc/services.d/qsh-sniffer/finish \
    && chmod a+x /opt/qsh/cosy6_decoder.py
