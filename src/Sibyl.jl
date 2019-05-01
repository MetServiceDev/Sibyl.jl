module Sibyl

using AMPSBase.Log

using SHA
using CodecZlib
using TranscodingStreams
import AWSCore
import AWSS3
using Nullables
using Distributed

import Base.keys
import Base.haskey
import Base.getindex
import Base.delete!

include("base62.jl")

const S3CONNECTIONS = 128
const TIMEOUTLIMIT = 32
const TIMEOUTINCREMENT = 8

const AWSEnv=Dict

export asbytes,frombytes
export empty

const Bytes=Array{UInt8,1}
const empty=Bytes()

abstract type SibylCache end
writecache(cache::SibylCache,key::String,data::Bytes)=error("writecache not implemented")
readcache(cache::SibylCache,key::String)=error("readcache not implemented")

include("nocache.jl")
include("fscache.jl")

mutable struct GlobalEnvironment
    awsenv::Nullable{AWSEnv}
    s3connections::Base.Semaphore
    cache::SibylCache
    mtimes::Dict{Tuple{String,String},Tuple{Int,Int}}
    touchmtimes::Bool
    forcecompact::Bool
    nevercompact::Bool
    mtimelock::Base.Semaphore
    getawsenvlock::Base.Semaphore
    timeoutlimit::Int
    timeoutincrement::Int
    
    putcnt::Int
    getcnt::Int
    lstcnt::Int
    delcnt::Int
end

const globalenv=GlobalEnvironment(Nullable{AWSEnv}(),
                                  Base.Semaphore(S3CONNECTIONS),
                                  FSCache.Cache(),
                                  Dict{Tuple{String,String},Tuple{Int,Int}}(),
                                  true,false,false,
                                  Base.Semaphore(1),
                                  Base.Semaphore(1),
                                  TIMEOUTLIMIT,
                                  TIMEOUTINCREMENT,
                                  0,0,0,0)

function reset_locks()
    global globalenv
    globalenv.s3connections = Base.Semaphore(S3CONNECTIONS)
    globalenv.mtimelock = Base.Semaphore(1)
    globalenv.getawsenvlock = Base.Semaphore(1)
end

function __init__()
    global makeawsenv=defaultmakeawsenv
end

function setmakeawsenv(f)
    global makeawsenv
    makeawsenv=f
end

function defaultmakeawsenv()
    if haskey(ENV,"AWS_ID")
        Nullable{AWSEnv}(AWSCore.aws_config(creds=AWSCore.AWSCredentials(ENV["AWS_ID"],ENV["AWS_SECKEY"])))
    else
        Nullable{AWSEnv}(AWSCore.aws_config())
    end
end

function getnewawsenv()
    global globalenv
    globalenv.awsenv=makeawsenv()
    return get(globalenv.awsenv)
end

function getawsenv()
    global globalenv
    if isnull(globalenv.awsenv)
        return getnewawsenv()
    end
    return get(globalenv.awsenv)
end

function acquires3connection()
    global globalenv
    yield()
    Base.acquire(globalenv.s3connections)
end

function releases3connection()
    global globalenv
    Base.release(globalenv.s3connections)
end

function acquiregetawsenvlock()
    global globalenv
    yield()
    Base.acquire(globalenv.getawsenvlock)
end

function releasegetawsenvlock()
    global globalenv
    Base.release(globalenv.getawsenvlock)
end

"""
@timeout secs expr then pollint
Start executing `expr`; if it doesn't finish executing in `secs` seconds,
then execute `then`. `pollint` controls the amount of time to wait in between
checking if `expr` has finished executing (short for polling interval).
"""
macro timeout(t, expr, then, pollint=0.1)
    return quote
        if $(esc(t)) == Inf
            $(esc(expr))
        else
            tm = Float64($(esc(t)))
            start = time()
            tsk = @async $(esc(expr))
            yield()
            while !istaskdone(tsk) && (time() - start < tm)
                sleep($pollint)
            end
            if istaskdone(tsk)
                tsk.result
            else
                $(esc(then))
            end
        end
    end
end

function s3putobject(bucket,s3key,m)
    globalenv.putcnt+=1
    trycount=0
    while true
        try
            acquiregetawsenvlock()
            env=getawsenv()
            releasegetawsenvlock()
            AWSS3.s3_put(env,bucket,s3key,m)
            return 0
        catch e
            @logwarn "The following exception was caught in Sibyl.s3putobject"
            @logwarn e
        end
        if trycount>0
            try
                acquiregetawsenvlock()
                getnewawsenv()
                releasegetawsenvlock()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            error("s3putobject timed out.")
        end
    end
end

function s3getobject1(bucket,s3key)
    globalenv.getcnt+=1
    trycount=0
    while true
        try
            acquiregetawsenvlock()
            env=getawsenv()
            releasegetawsenvlock()
            r=AWSS3.s3_get(env,bucket,s3key)
            return r
        catch e
            if (e isa AWSCore.AWSException)&&(e.code=="NoSuchKey")
                return empty
            end
            @logwarn e
        end
        if trycount>0
            try
                acquiregetawsenvlock()
                getnewawsenv()
                releasegetawsenvlock()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            error("s3getobject timed out.")
        end
    end
end

function s3getobject(bucket,s3key)
    cachekey="OBJ:$(bucket):$(s3key)"
    cached=readcache(globalenv.cache,cachekey)
    if !isnull(cached)
        return get(cached)[2]
    end
    value=s3getobject1(bucket,s3key)
    writecache(globalenv.cache,cachekey,value)
    return value
end

function s3deleteobject(bucket,s3key)
    globalenv.delcnt+=1
    acquires3connection()
    try
        env=getawsenv()
        AWSS3.s3_delete(env,bucket,s3key)
    catch
    end
    releases3connection()
end

function s3listobjects1(bucket,prefix)
    globalenv.lstcnt+=1
    trycount=0
    acquires3connection()
    while true
        try
            env=getawsenv()
            r=String[]
            q=Dict("prefix"=>prefix)
            while true
                resp=AWSS3.s3(env,"GET",bucket;query=q)
                if haskey(resp,"Contents")
                    if isa(resp["Contents"],Array)
                        for x in resp["Contents"]
                            push!(r,x["Key"])
                            q["marker"]=x["Key"]
                        end
                    else
                        push!(r,resp["Contents"]["Key"])
                        q["marker"]=resp["Contents"]["Key"]
                    end
                end
                if resp["IsTruncated"]!="true"
                    releases3connection()
                    return r
                end
            end
        catch e
            @logwarn "The following exception was caught in Sibyl.s3listobjects1"
            @logwarn e
        end
        if trycount>0
            try
                getnewawsenv()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            releases3connection()
            error("s3listobjects timed out.")
        end
    end
end

function touchmtimes(bucket,s3key)
    if !globalenv.touchmtimes
        return
    end
    s=split(s3key,'/')
    space=join(s[1:(end-5)],'/')
    table=s[end-4]
    myhash=s[end-3]
    m=asbytes(Int64(round(time())))
    timeoutlimit=globalenv.timeoutlimit
    todo=Set(0:4)
    while length(todo)>0
        tempresults=Dict()
        @sync for i in todo
            result=Future()
            tempresults[i]=result
            prefix=join([space,table,"mtime",myhash[1:i]],'/')
            @async put!(result, 
                @timeout(timeoutlimit,
                         s3putobject(bucket,prefix,m),
                         begin
                            uniqueid = hash([bucket,key])
                            @loginfo "    touchmtimes timed out: $(bucket) $(uniqueid)"
                            nothing
                         end
                    )
                )
        end
        for (i,result) in tempresults
            r=fetch(result)
            if r!=nothing
                pop!(todo,i)
            end
        end
        timeoutlimit+=globalenv.timeoutincrement
    end
end

function getmtime(bucket,s3prefix)
    s=split(s3prefix,"/")
    space=join(s[1:(end-3)],'/')
    table=s[end-2]
    myhash=s[end-1]
    if haskey(globalenv.mtimes,(space,table))
        if globalenv.mtimes[(space,table)][1]+60>time()
            return globalenv.mtimes[(space,table)][2]
        end
    end
    Base.acquire(globalenv.mtimelock)
    if haskey(globalenv.mtimes,(space,table))
        if globalenv.mtimes[(space,table)][1]+60>time()
            Base.release(globalenv.mtimelock)
            return globalenv.mtimes[(space,table)][2]
        end
    end    
    val=try
        acquires3connection()
        frombytes(s3getobject1(bucket,join([space,table,"mtime",""],'/')),Int64)[1]
        releases3connection()
    catch
        Int64(0)
    end
    globalenv.mtimes[(space,table)]=(Int64(round(time())),val)
    Base.release(globalenv.mtimelock)
    return val
end

function s3listobjects(bucket,prefix)
    cachekey="LIST:$(bucket):$(prefix)"
    cached=readcache(globalenv.cache,cachekey)
    if !isnull(cached)
        if get(cached)[1]>getmtime(bucket,prefix)
            return frombytes(get(cached)[2],Array{String,1})[1]
        end
    end
    value=convert(Array{String,1},s3listobjects1(bucket,prefix))
    writecache(globalenv.cache,cachekey,asbytes(value))
    return value    
end

mutable struct Connection
    bucket::String
    space::String
end

function writebytes(io,xs...)
    for x in xs
        if typeof(x)<:AbstractString
            b=IOBuffer()
            write(b,String(x))
            b=take!(b)
            write(io,Int16(length(b)))
            write(io,b)
        elseif typeof(x) in [Array{String,1},Array{Array{UInt8,1},1}]
            write(io,Int16(length(x)))
            for e in x
                writebytes(io,e)
            end
        elseif typeof(x)<:Array
            b=reinterpret(UInt8,x)
            write(io,Int64(length(b)))
            write(io,b)
        else
            write(io,x)
        end
    end
end

frombytesarray(data::Bytes,typ::Type{Array{T,1}}) where T=reinterpret(T,data)

function readbytes(io,typs...)
    r=[]
    for typ in typs
        if typ==String
            l=read(io,Int16)
            b=Array{UInt8}(undef,l)
            read!(io,b)
            push!(r,String(b))
        elseif typ==Array{String,1}
            l=read(io,Int16)
            a=Array{String,1}()
            for i=1:l
                push!(a,readbytes(io,String)[1])
            end
            push!(r,a)
        elseif typ==Array{Array{UInt8,1},1}
            l=read(io,Int16)
            a=Array{UInt8,1}[]
            for i=1:l
                push!(a,readbytes(io,Array{UInt8,1})[1])
            end
            push!(r,a)
        elseif typ<:Array
            l=read(io,Int64)
            b=Array{UInt8}(undef,l)
            read!(io,b)
            push!(r,frombytesarray(b,typ))
        else
            push!(r,read(io,typ))
        end
    end
    return r
end

function readBytes(io)
    l=read(io,Int64)
    b=Array{UInt8}(undef,l)
    read!(io,b)
    return b
end

function readString(io)
    l=read(io,Int16)
    b=Array{UInt8}(undef,l)
    read!(io,b)
    return String(b)
end

function asbytes(xs...)
    io=IOBuffer()
    writebytes(io,xs...)
    return take!(io)
end

function frombytes(data,typs...)
    io=IOBuffer(data)
    return readbytes(io,typs...)
end
    
mutable struct BlockTransaction
    data::Dict{Bytes,Bytes}
    deleted::Set{Bytes}
    s3keystodelete::Array{String,1}
end

keys(t::BlockTransaction)=keys(t.data)
haskey(t::BlockTransaction,k)=haskey(t.data,k)
getindex(t::BlockTransaction,k)=getindex(t.data,k)


BlockTransaction()=BlockTransaction(Dict{Bytes,Bytes}(),Set{Bytes}(),Array{String,1}())

function upsert!(t::BlockTransaction,subkey::Bytes,value::Bytes)
    delete!(t.deleted,subkey)
    t.data[subkey]=value
end

function delete!(t::BlockTransaction,subkey::Bytes)
    if haskey(t.data,subkey)
        delete!(t.data,subkey)
    end
    push!(t.deleted,subkey)
end

function message(t::BlockTransaction)
    rawbuf=IOBuffer()
    io=GzipCompressorStream(rawbuf)
    writebytes(io,Int64(length(keys(t.data))))
    for (k,v) in t.data
        writebytes(io,k,v)
    end
    writebytes(io,Int64(length(t.deleted)))
    for k in t.deleted
        writebytes(io,k)
    end
    writebytes(io,Int64(length(t.s3keystodelete)))
    for s in t.s3keystodelete
        writebytes(io,s)
    end
    write(io,TranscodingStreams.TOKEN_END) # finalise compression
    return take!(rawbuf)
end

function interpret!(t::BlockTransaction,message::Bytes)
    if length(message)==0
        return
    end
    io=GzipDecompressorStream(IOBuffer(message))
    n=read(io,Int64)
    sizehint!(t.data,n)
    for i=1:n
        x1=readBytes(io)
        x2=readBytes(io)
        t.data[x1]=x2
    end
    n=read(io,Int64)
    for i=1:n
        delete!(t.data,readBytes(io))
    end
    n=read(io,Int64)
    for i=1:n
        push!(t.s3keystodelete,readString(io))
    end
end

mutable struct Transaction
    connection::Connection
    tables::Dict{String,Dict{Bytes,BlockTransaction}}
end

keys(t::Transaction)=keys(t.tables)
haskey(t::Transaction,k)=haskey(t.tables,k)
getindex(t::Transaction,k)=getindex(t.tables,k)

Transaction(connection)=Transaction(connection,Dict{String,Dict{Bytes,BlockTransaction}}())

function upsert!(t::Transaction,table::AbstractString,key::Bytes,subkey::Bytes,value::Bytes)
    if !(haskey(t.tables,table))
        t.tables[table]=Dict{Bytes,BlockTransaction}()
    end
    if !(haskey(t.tables[table],key))
        t.tables[table][key]=BlockTransaction()
    end
    upsert!(t.tables[table][key],subkey,value)
end

function delete!(t::Transaction,table::AbstractString,key::Bytes,subkey::Bytes)
    if !(haskey(t.tables,table))
        t.tables[table]=Dict{Bytes,BlockTransaction}()
    end
    if !(haskey(t.tables[table],key))
        t.tables[table][key]=BlockTransaction()
    end
    delete!(t.tables[table][key],subkey)
end

function s3keyprefix(space,table,key)
    myhash=bytes2hex(sha256(key))[1:4]
    return "$(space)/$(table)/$(myhash)/$(Base62.encode(key))"
end

function saveblock(blocktransaction::BlockTransaction,connection,table,key)
    s3prefix=s3keyprefix(connection.space,table,key)
    m=message(blocktransaction)
    timestamp=Base62.encode(asbytes(Int64(round(time()))))
    nonce=Base62.encode(sha256(m))
    s3key="$(s3prefix)/$(timestamp)/$(nonce)"
    s3putobject(connection.bucket,s3key,m)
    touchmtimes(connection.bucket,s3key)
    return 0
end

function save(t::Transaction)
    timeoutlimit=globalenv.timeoutlimit
    @sync for (table,blocktransactions) in t.tables
        todo=Set(blocktransactions)
        while length(todo)>0
            tempresults=Dict()
            @sync for object in todo
                result=Future()
                tempresults[object]=result
                (key,blocktransaction) = object
                @async begin
                    acquires3connection()
                    put!(result,
                         @timeout(timeoutlimit,
                                  saveblock(blocktransaction, t.connection, table, key),
                                  begin
                                      uniqueid = hash([table,key,blocktransaction])
                                      @loginfo "    save timed out: $(table) $(uniqueid)"
                                      nothing
                                  end
                             )
                         )
                    releases3connection()
                end
            end
            for (object,result) in tempresults
                r=fetch(result)
                if r!=nothing
                    pop!(todo,object)
                end
            end
            timeoutlimit+=globalenv.timeoutincrement
        end
    end
end

function readblock(connection::Connection,table::AbstractString,key::Bytes)
    timeoutlimit=globalenv.timeoutlimit
    objects=[(frombytes(Base62.decode(String(split(x,"/")[end-1])),Int64)[1],
              split(x,"/")[end],x)
             for x in s3listobjects(connection.bucket,s3keyprefix(connection.space,table,key))]
    sort!(objects)
    results=[]
    todo=Set(objects)
    while length(todo)>0
        tempresults=Dict()
        @sync for object in todo
            result=Future()
            tempresults[object]=result
            @async begin
                acquires3connection()
                put!(result,
                     @timeout(timeoutlimit,
                        s3getobject(connection.bucket,object[3]),
                        begin
                            uniqueid = hash([table,key,object])
                            @loginfo "    readblock timed out: $(table) $(uniqueid)"
                            nothing
                        end
                        )
                     )
                releases3connection()
            end
        end
        for (object,result) in tempresults
            r=fetch(result)
            if r!=nothing
                pop!(todo,object)
                push!(results,r)
            end
        end
        timeoutlimit+=globalenv.timeoutincrement
    end
    r=BlockTransaction()
    for result in results
        interpret!(r,result)
    end
    if !(globalenv.nevercompact)
        s3livekeys=String[]
        @sync for x in objects
            if x[3] in r.s3keystodelete
                @async s3deleteobject(connection.bucket,x[3])
                touchmtimes(connection.bucket,x[3])
            else
                push!(s3livekeys,x[3])
            end
        end
        compactprobability=(length(s3livekeys)-1)/(length(s3livekeys)+100)
        if (length(s3livekeys)>=2)&&(globalenv.forcecompact)
            compactprobability=1.0
        end
        if rand()<compactprobability
            newblock=BlockTransaction(r.data,r.deleted,s3livekeys)
            acquires3connection()
            saveblock(newblock,connection,table,key)
            releases3connection()
        end
    end
    return r
end

function deletekey(connection::Connection,table::String,key::Bytes)
    objects=[(frombytes(Base62.decode(String(split(x,"/")[end-1])),Int64)[1],
              split(x,"/")[end],x)
             for x in s3listobjects(connection.bucket,s3keyprefix(connection.space,table,key))]
    @sync for x in objects
        @async s3deleteobject(connection.bucket,x[3])
        touchmtimes(connection.bucket,x[3])
    end
end

function loadblocks!(t::Transaction,tablekeys)
    results=[]
    for (table,key) in tablekeys
        result=Future()
        push!(results,result)
        @async put!(result,readblock(t.connection,table,key))
    end
    for i=1:length(tablekeys)
        if !haskey(t.tables,tablekeys[i][1])
            t.tables[tablekeys[i][1]]=Dict{Bytes,BlockTransaction}()
        end
        t.tables[tablekeys[i][1]][tablekeys[i][2]]=fetch(results[i])
    end
end

function compact(bucket,space;table="",marker="",reportfunc=println)
    globalenv.cache=NoCache.Cache()
    globalenv.s3connections=Base.Semaphore(512)
    globalenv.forcecompact=true
    connection=Connection(bucket,space)
    env=getawsenv()
    slashesinspace=length(split(space,"/"))-1
    prefix=if table==""
        "$(space)/"
    else
        "$(space)/$(table)/"
    end
    q=Dict("prefix"=>prefix)
    if marker!=""
        q["marker"]=marker
    end
    r=String[]
    resp=AWSS3.s3(env,"GET",bucket;query=q)
    if haskey(resp,"Contents")
        if isa(resp["Contents"],Array)
            for x in resp["Contents"]
                push!(r,x["Key"])
            end
        else
            push!(r,resp["Contents"]["Key"])
        end
    end
    K=[]
    for x in r
        try
            s=split(x,"/")
            if !(s[3+slashesinspace] in ["mtime","raw"])
                push!(K,(s[2+slashesinspace],Base62.decode(s[4+slashesinspace])))
            end
        catch
        end
    end
    while length(K)>0
        k=popfirst!(K)
        cnt=1
        while (length(K)>0)&&(k==K[1])
            popfirst!(K)
            cnt=cnt+1
        end
        if cnt>1
            reportfunc("$(space) $(k[1]) /$(Base62.encode(k[2]))/ $(cnt)")
            let k=k
                readblock(connection,k[1],k[2])
            end
        end
    end
    if length(r)==1000
        reportfunc(r[end])
        return r[end]
    else
        return ""
    end
end

end
