FROM docker:28.5.1-cli AS docker_cli
FROM kestra/kestra:v1.3.37

USER root
COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
