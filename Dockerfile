# -----------------------------------------------------------------------------
#  Stage 0: build sqlite binary
# -----------------------------------------------------------------------------
FROM ubuntu:22.04 AS sqlite

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# install sqlite
COPY test_sqlite3.sh test_sqlite3.sh

RUN apt-get update && \
    apt-get install build-essential -y && \
    apt-get install wget -y
RUN wget https://www.sqlite.org/2023/sqlite-autoconf-3410100.tar.gz
RUN tar -xvf sqlite-autoconf-3410100.tar.gz
RUN ./sqlite-autoconf-3410100/configure && \
    make && \
    make install
RUN sqlite3 --version && \
    ./test_sqlite3.sh


# -----------------------------------------------------------------------------
#  Stage 1: install pyenv
# -----------------------------------------------------------------------------
FROM ubuntu:22.04 AS pyenv

ENV DEBIAN_FRONTEND=noninteractive

# install pyenv with pyenv-installer
COPY pyenv_dependencies.txt pyenv_dependencies.txt

ENV PYENV_GIT_TAG=v2.3.14

RUN apt-get update && \
    apt-get install -y $(cat pyenv_dependencies.txt) && \
    apt-get install curl -y
RUN curl https://pyenv.run | bash
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN pyenv install 3.10 && \
    pyenv global 3.10

# # -----------------------------------------------------------------------------
# #  Stage 2: user setup
# # -----------------------------------------------------------------------------
FROM ubuntu:22.04

COPY --from=sqlite /usr/local/bin/sqlite3 /usr/local/bin/sqlite3
COPY --from=sqlite /usr/local/lib/libsqlite3.so.0 /usr/local/lib/libsqlite3.so.0
COPY test_sqlite3.sh test_sqlite3.sh
RUN sqlite3 --version && \
    ./test_sqlite3.sh

COPY --from=pyenv /root/.pyenv /root/.pyenv
ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH
ENV PYTHONIOENCODING utf-8

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y nano

ENV DATA /home/jimena/work/data
ENV LFFOLDER $DATA/fundpet/linkfluence_petitioning_fundraising

# clone project repo
ARG token
ENV env_token $token
WORKDIR /home/jimena/work/dev
RUN git clone https://${env_token}@github.com/jimenaRL/fundpet.git
WORKDIR /home/jimena/work/dev/fundpet

RUN pip install tqdm==4.66.1
RUN pip install PyYAML==6.0.1
RUN pip install pandas==2.1.2
RUN pip install minet==1.1.6
RUN pip install trafilatura==1.6.2
RUN pip install beautifulsoup4==4.12.2
RUN pip install requests==2.31.0


# build with
# docker build -t fundpet --build-arg token=$GIT_TOKEN -f Dockerfile .

# run with
# docker run -d --env-file=credentials.env -v /home/jimena/work/dev/fundpet/wip:/home/jimena/work/dev/fundpet/wip fundpet bash -c "python retrieve_past.py --query=SoMe4DemFrench"


