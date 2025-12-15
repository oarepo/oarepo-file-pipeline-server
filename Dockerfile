FROM python:3.13-alpine

ARG REPOSITORY_SITE_ORGANIZATION
ARG REPOSITORY_SITE_NAME
ARG REPOSITORY_IMAGE_URL
ARG REPOSITORY_AUTHOR
ARG REPOSITORY_GITHUB_URL
ARG REPOSITORY_URL
ARG REPOSITORY_DOCUMENTATION
ARG DEPLOYMENT_VERSION

LABEL maintainer="${REPOSITORY_SITE_ORGANIZATION}" \
    org.opencontainers.image.authors="${REPOSITORY_AUTHOR}" \
    org.opencontainers.image.title="${REPOSITORY_SITE_NAME} production image" \
    org.opencontainers.image.url="${REPOSITORY_IMAGE_URL}" \
    org.opencontainers.image.source="${REPOSITORY_GITHUB_URL}" \
    org.opencontainers.image.documentation="${REPOSITORY_DOCUMENTATION}"

# Install build dependencies for uwsgi and other packages
RUN apk add python3-dev \
    build-base \
    linux-headers \
    pcre-dev \
    libffi-dev \
    openssl-dev \
    git

# Install the package and its dependencies
COPY pyproject.toml /app/
COPY oarepo_file_pipeline_server /app/oarepo_file_pipeline_server/
WORKDIR /app

# Install the package in development mode
RUN pip install --no-cache-dir -e .

# Copy uwsgi configuration and startup script
COPY uwsgi.ini /uwsgi.ini
COPY uwsgi-script.py /uwsgi-script.py

# Expose the server port
EXPOSE 5555

# Show installed packages for debugging
RUN pip list && which uwsgi

# Start uwsgi
CMD ["/usr/local/bin/uwsgi", "--ini", "/uwsgi.ini"]
