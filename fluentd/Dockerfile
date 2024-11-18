FROM fluent/fluentd:v1.16-1

# Switch to root user for installation
USER root

# Install build tools and dependencies
RUN apk add --no-cache --update \
    build-base \
    ruby-dev \
    ruby-bundler \
    && gem install \
    fluent-plugin-kafka \
    fluent-plugin-elasticsearch \
    && gem sources --clear-all \
    && apk del build-base ruby-dev \
    && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

# Switch back to fluent user
USER fluent