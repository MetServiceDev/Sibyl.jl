module FSCache

using SHA

import ..SibylCache
import ..Bytes
import ..writecache
import ..readcache

mutable struct Cache <: SibylCache
    basepath::String
end

function Cache()
    home=ENV["HOME"]
    basepath="$(home)/.sibyl"
    return Cache(basepath)
end

function keytopath(cache::Cache,key::String)
    h=bytes2hex(sha256(key))
    join([cache.basepath,h[1:2],h[1:4],h],"/")
end

function writecache(cache::Cache,key::String,data::Bytes)
    path=keytopath(cache,key)
    dir=splitdir(path)[1]
    if !isdir(dir)
        mkpath(dir)
    end
    f=open(path*".tmp","w")
    write(f,Int64(round(time())))
    write(f,sha256(data))
    write(f,data)
    close(f)
    mv(path*".tmp",path,remove_destination=true)
end

function readcache(cache::Cache,key::String)
    path=keytopath(cache,key)
    if isfile(path)
        f=open(path)
        ctime=read(f,Int64)
        h=read(f,UInt8,32)
        data=read(f)
        close(f)
        if h==sha256(data)
            return Nullable{Tuple{Int64,Bytes}}((ctime,data))
        end
    end
    return Nullable{Tuple{Int64,Bytes}}()
end


end
