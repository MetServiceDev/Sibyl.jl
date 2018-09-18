module NoCache

using Nullables

import ..SibylCache
import ..Bytes
import ..writecache
import ..readcache

mutable struct Cache <: SibylCache
end

function writecache(cache::Cache,key::String,data::Bytes)
#    println("Write to NoCache $(key)")
end

function readcache(cache::Cache,key::String)
#    println("Read from NoCache $(key)")
    return Nullable{Tuple{Int64,Bytes}}()
end

end
