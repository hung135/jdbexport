#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM openjdk:11-jdk

# This Dockerfile adds a non-root 'vscode' user with sudo access. However, for Linux,
# this user's GID/UID must match your local user UID/GID to avoid permission issues
# with bind mounts. Update USER_UID / USER_GID if yours is not 1000. See
# https://aka.ms/vscode-remote/containers/non-root-user for details.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

#COPY release-manager/requirements* /tmp/pip-tmp/

# Configure apt
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
  #
  # Create a non-root user to use if preferred - see https://aka.ms/vscode-remote/containers/non-root-user.
  && groupadd --gid $USER_GID $USERNAME \
  && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
  # [Optional] Add sudo support for the non-root user
  && apt-get install -y sudo \
  && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
  && chmod 0440 /etc/sudoers.d/$USERNAME \
  #
  # Verify git, needed tools installed
  && apt-get -y install git iproute2 procps curl lsb-release vim fish zip

#-------------------Uncomment the following steps to install Maven CLI Tools----------------------------------
ARG MAVEN_VERSION=3.6.1
ARG MAVEN_SHA=b4880fb7a3d81edd190a029440cdf17f308621af68475a4fe976296e71ff4a4b546dd6d8a58aaafba334d309cc11e638c52808a4b0e818fc0fd544226d952544
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${MAVEN_SHA} /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
COPY maven-settings.xml /usr/share/maven/ref/
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG /root/.m2
#-------------------------------------------------------------------------------------------------------------
# Install Docker CE CLI
RUN apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common lsb-release \
  && curl -fsSL https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]')/gpg | (OUT=$(apt-key add - 2>&1) || echo $OUT) \
  && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]') $(lsb_release -cs) stable" \
  && apt-get update \
  && apt-get install -y docker-ce-cli fish vim postgresql-client \
  #
  # Install Docker Compose
  && curl -sSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
  && chmod +x /usr/local/bin/docker-compose 
#-------------------Uncomment the following steps to install Gradle CLI Tools---------------------------------
# ENV GRADLE_HOME /opt/gradle
# ENV GRADLE_VERSION 5.4.1
# ARG GRADLE_DOWNLOAD_SHA256=7bdbad1e4f54f13c8a78abc00c26d44dd8709d4aedb704d913fb1bb78ac025dc
# RUN curl -sSL --output gradle.zip "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip" \
#     && echo "${GRADLE_DOWNLOAD_SHA256} *gradle.zip" | sha256sum --check - \
#     && unzip gradle.zip \
#     && rm gradle.zip \
#     && mv "gradle-${GRADLE_VERSION}" "${GRADLE_HOME}/" \
#     && ln -s "${GRADLE_HOME}/bin/gradle" /usr/bin/gradle
#-------------------------------------------------------------------------------------------------------------

# Clean up
# RUN apt-get autoremove -y \
#     && apt-get clean -y \
#     && rm -rf /var/lib/apt/lists/*
#ENV DEBIAN_FRONTEND=

# Release manager setup
RUN apt-get install -y python3-pip
#RUN pip3 install -r /tmp/pip-tmp/requirements.txt

# Allow for a consistant java home location for settings - image is changing over time
RUN if [ ! -d "/docker-java-home" ]; then ln -s "${JAVA_HOME}" /docker-java-home; fi
RUN wget http://search.maven.org/remotecontent?filepath=org/python/jython-standalone/2.7.1/jython-standalone-2.7.1.jar -O /opt/jython27.jar

RUN echo 'alias java="/usr/local/openjdk-11/bin/java"' >> ~/.bashrc
RUN echo 'alias jython="/usr/local/openjdk-11/bin/java -jar /opt/jython27.jar"' >> ~/.bashrc
RUN echo 'alias buildjar="mvn compile assembly:single -f /workspace/pom.xml && cp /workspace/target/DbTest-jar-with-dependencies.jar /workspace/jython_scripts/DbTest-jar-with-dependencies.jar"' >>~/.bashrc
RUN echo 'alias buildjarclean="mvn clean compile assembly:single -f /workspace/pom.xml"' >>~/.bashrc
RUN echo 'alias runjar="java -jar ${JAREXPORT}"' >>~/.bashrc
RUN echo 'alias jartest="mvn clean test"' >> ~/.bashrc
RUN echo 'alias mvntest="mvn test -f /workspace/pom.xml"' >> ~/.bashrc
RUN echo 'alias buildrunjython="mvn compile assembly:single && /usr/local/openjdk-11/bin/java -jar /opt/jython27.jar"' >>~/.bashrc 
RUN echo 'alias runreports="cd /workspace/jython_scripts/ && jython dbdiff.py -y conn.yaml -t reports.yaml"' >>~/.bashrc 
RUN echo 'alias runtasks="cd /workspace/jython_scripts/ && jython dbdiff.py -y conn.yaml -t tasks.yaml"' >>~/.bashrc 
RUN echo 'alias release="/workspace/.devcontainer/releasepackage.sh"'>>~/.bashrc
# Release manager
# RUN echo 'alias release="python3 /workspace/release-manager/pyGithubManager.py ${GITHUBRELEASE} -k ${GITHUBKEY} -r {REPO}"' >>~/.bashrc
# RUN echo 'alias releaseparm="release -c \"${git symbolic-ref --short HEAD}\" -m \"${date -I}\" -a ${JAREXPORT}"' >> ~/.bashrc
# UN echo 'alias package="rm -f /workspace/dbdiff.tar && cd /workspace/jython_scripts/ && tar -cvf /workspace/dbdiff.tar . && cd /workspace/ && tar -rvf dbdiff.tar ${JAREXPORT}"' >> ~/.bashrc
RUN echo 'alias package="rm -f /workspace/dbdiff.zip && cd /workspace/jython_scripts/ && zip -r /workspace/dbdiff.zip . && cd /workspace/ && zip -ur dbdiff.zip ${JAREXPORT}"' >> ~/.bashrc
RUN echo 'alias releasepackage="bash /workspace/.devcontainer/releasepackage.sh"' >> ~/.bashrc
RUN echo 'alias deletereleases="python3 ${GITHUBRELEASE} -v -k ${GITHUBKEY} -r ${REPO} -da"' >> ~/.bashrc

# RUN echo 'alias test="mvn test"' >>~/.bashrc
RUN wget https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh
RUN wget https://scan.coverity.com/download/linux64 --post-data "token=6GQCVP3FzRjnpDIR8XGGFg&project=hung135%2Fdbdifftest" -O /tmp/coverity_tool.tgz
RUN tar -xvf /tmp/coverity_tool.tgz -C /tmp/ 

RUN echo 'alias coverity="/tmp/cov-analysis-linux64-2019.03/bin/cov-build --dir cov-int mvn compile assembly:single -f /workspace/pom.xml"' >>~/.bashrc
RUN echo 'alias tarcoverity="tar czvf myproject.tgz cov-int"' >>~/.bashrc
RUN echo 'source /git-prompt.sh' >>~/.bashrc
RUN echo 'source /workspace/env.rc' >>~/.bashrc
RUN echo 'alias submitcoverity="curl --form token=6GQCVP3FzRjnpDIR8XGGFg \
  --form email=hung135@hotmail.com \
  --form file=@myproject.tgz \
  --form version="Version" \
  --form description="Description" \
  https://scan.coverity.com/builds?project=hung135%2Fdbdifftest"'>>~/.bashrc

RUN echo 'alias sonar="mvn sonar:sonar \
  -Dsonar.projectKey=com.hung135 \
  -Dsonar.host.url=http://sonarqube:9000 \
  -Dsonar.login=f91baa6ba91b88f93c8a493405c2b79d15cc8bde"'>>/.bashrc