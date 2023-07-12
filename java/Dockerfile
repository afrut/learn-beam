FROM ubuntu:mantic-20230624
SHELL ["/bin/bash", "-c"]
RUN apt update && \
    apt install -y curl && \
    apt install -y unzip && \
    apt install -y zip && \
    curl -s "https://get.sdkman.io" | bash && \
    source "/root/.sdkman/bin/sdkman-init.sh" && \
    sdk install java 17.0.5-tem && \
    sdk install maven