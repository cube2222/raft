FROM alpine:latest
RUN mkdir /app
ADD cmd/cmd /app/
WORKDIR /app
EXPOSE 8002
CMD ["/app/cmd"]