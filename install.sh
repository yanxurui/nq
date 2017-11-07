set -e

mkdir -p temp
cd temp

### 1. install luajit2.1 to /usr/local ###
wget -nc http://luajit.org/download/LuaJIT-2.1.0-beta2.tar.gz
tar -xzf LuaJIT-2.1.0-beta2.tar.gz
cd LuaJIT-2.1.0-beta2
make
make install
export LUAJIT_LIB=/usr/local/lib
export LUAJIT_INC=/usr/local/include/luajit-2.1
cd ..

# download NDK, lua-nginx, nginx
wget -nc https://github.com/simpl/ngx_devel_kit/archive/v0.3.0.tar.gz
tar -xzf v0.3.0.tar.gz

wget -nc https://github.com/openresty/lua-nginx-module/archive/v0.10.11.tar.gz
tar -xzf v0.10.11.tar.gz

wget -nc "http://nginx.org/download/nginx-1.13.6.tar.gz"
tar -xzf nginx-1.13.6.tar.gz

### 2. compile & install nginx to /opt/nginx ###
cd nginx-1.13.6
./configure --prefix=/opt/nginx \
        --with-ld-opt="-Wl,-rpath,/usr/local/lib" \
        --add-module=../ngx_devel_kit-0.3.0 \
        --add-module=../lua-nginx-module-0.10.11
make
make install
cd ..

cd ..

mkdir -p logs

### 3. install dependencies ###
# install resty libraries
cp -r deps/lua-resty-core/lib ./
cp -r deps/lua-resty-lrucache/lib ./
cp -r deps/lua-resty-mysql/lib ./

# install inspect.lua
cp deps/inspect/inspect.lua ./lib/

# compile and install lua-cjson
mkdir -p clib

pushd deps/lua-cjson
cp Makefile Makefile.old
git apply ../cjson-diff.txt
make
mv Makefile.old Makefile
popd
cp deps/lua-cjson/cjson.so ./clib/

echo 'successfully'
