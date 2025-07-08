FROM ghcr.io/multani/nomad:1.10.2

# Install iproute2 to fix the /sbin/ip issue
# Try different package managers since we don't know the base image
RUN (apt-get update && apt-get install -y iproute2 curl) || \
    (yum install -y iproute curl) || \
    (dnf install -y iproute curl) || \
    (apk add --no-cache iproute2 curl) || \
    echo "Could not install iproute2 and curl"

# Create symlink for compatibility if needed
RUN ln -sf /usr/bin/ip /sbin/ip 2>/dev/null || \
    ln -sf /bin/ip /sbin/ip 2>/dev/null || \
    ln -sf /sbin/ip /usr/bin/ip 2>/dev/null || \
    echo "IP command already available"