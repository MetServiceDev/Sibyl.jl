module Sibyl

using SHA
using Zlib
using AWS
using AWS.S3

export asbytes
export empty

typealias Bytes Array{UInt8,1}

const empty=Bytes()

function writebytes(io,x::String)
    b=IOBuffer()
    write(b,UTF8String(x))
    b=takebuf_array(b)
    write(io,Int16(length(b)))
    write(io,b)
end

writebytes(io,x::Int64)=write(io,x)
writebytes(io,x::Float64)=write(io,x)

function asbytes(xs...)
    io=IOBuffer()
    for x in xs
        writebytes(io,x)
    end
    return takebuf_array(io)
end

type BlockTransaction
    data::Dict{Bytes,Bytes}
    deleted::Set{Bytes}
    s3keystodelete::Array{UTF8String,1}
end

BlockTransaction()=BlockTransaction(Dict{Bytes,Bytes}(),Set{Bytes}(),Array{UTF8String,1}())

function upsert!(t::BlockTransaction,subkey::Bytes,value::Bytes)
    delete!(t.deleted,subkey)
    t.data[subkey]=value
end

function message(t::BlockTransaction)
    io=IOBuffer()
    write(io,Int64(length(keys(t.data))))
    for (k,v) in t.data
        write(io,Int64(length(k)))
        write(io,k)
        write(io,Int64(length(v)))
        write(io,v)
    end
    write(io,Int64(length(t.deleted)))
    for k in t.deleted
        write(io,Int64(length(k)))
        write(io,k)
    end
    write(io,Int64(length(t.s3keystodelete)))
    for s in t.s3keystodelete
        writebytes(io,s)
    end
    r=takebuf_array(io)
    return Zlib.compress(r,9)
end

type Transaction
    bucket::UTF8String
    space::UTF8String
    tables::Dict{UTF8String,Dict{Bytes,BlockTransaction}}
end

Transaction(bucket,space)=Transaction(bucket,space,Dict{UTF8String,Dict{Bytes,BlockTransaction}}())

function upsert!(t::Transaction,table::UTF8String,key::Bytes,subkey::Bytes,value::Bytes)
    if !(haskey(t.tables,table))
        t.tables[table]=Dict{Bytes,BlockTransaction}()
    end
    if !(haskey(t.tables[table],key))
        t.tables[table][key]=BlockTransaction()
    end
    upsert!(t.tables[table][key],subkey,value)
end

function s3keyprefix(space,table,key)
    base=",$(space),$(table),$(base64encode(key))"
    sha256(base)[1:4]*base
end

function save(env,t::Transaction)
    @sync for (table,blocktransactions) in t.tables
        for (key,blocktransaction) in blocktransactions
            s3prefix=s3keyprefix(t.space,table,key)
            m=message(blocktransaction)
            timestamp=base64encode(Int64(round(time())))
            s3key="$(s3prefix),$(timestamp),$(sha256(m))"
            @async begin
                S3.put_object(env,t.bucket,s3key,ASCIIString(m))
            end
        end
    end
end

end

