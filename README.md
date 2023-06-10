# memc-protobuf
Memcache Concurrent Data Load


follow:
https://www.geeksforgeeks.org/how-to-install-protocol-buffers-on-windows/
downloaded: protoc-23.2-win64.zip
compiled with a new version: protoc  --python_out=. ./appsinstalled.proto
protoc  --python_out=. ./appsinstalled.proto

docker run --name my-memcache -d memcached memcached -m 64

docker run -d --name memcached-container -p 11211:11211 ubuntu/memcached