FROM google/golang
RUN mkdir /compiled
RUN echo 'FROM scratch'                      > /compiled/Dockerfile && \
    echo "ADD gostalkd /gostalkd"           >> /compiled/Dockerfile && \
    echo "EXPOSE 40400"                     >> /compiled/Dockerfile && \
    echo "CMD [\"/gostalkd\"]"              >> /compiled/Dockerfile
ADD . /gopath/src/github.com/manveru/gostalk
WORKDIR /gopath/src/github.com/manveru/gostalk/gostalkd
RUN go get -t && \
    CGO_ENABLED=0 go build -a -ldflags '-s' -o /compiled/gostalkd && \
    tar -C /compiled -cf /compiled/compiled.tar .
CMD cat /compiled/compiled.tar
