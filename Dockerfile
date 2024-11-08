FROM openjdk:11
COPY ./dataset ./dataset
WORKDIR /release
COPY ./release/config ./config
COPY ./release/bin ./bin
COPY ./release/adapter ./adapter
ADD ./release/*.sh ./
ENTRYPOINT sh -c 'chmod 777 ./owner.sh' &&\
           sh -c './owner.sh start ./config/owner1.json' &&\
           sh -c 'tail -f /dev/null'