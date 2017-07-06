set -e

mkdir -p logs

### install dependencies ###
# install resty libraries
cp -r deps/lua-resty-core/lib ./
cp -r deps/lua-resty-lrucache/lib ./
cp -r deps/lua-resty-mysql/lib ./

# inspect.lua
cp deps/inspect/inspect.lua ./lib/

# compile and install lua-cjson
mkdir -p clib
make -C deps/lua-cjson
cp deps/lua-cjson/cjson.so ./clib/
