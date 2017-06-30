module Base62

function inttochar(i)
    if 0<=i<=9
        return '0'+i
    end
    if 10<=i<=35
        return 'A'-10+i
    end
    return 'a'-36+i
end

function chartoint(c)
    if '0'<=c<='9'
        return c-'0'
    end
    if 'A'<=c<='Z'
        return 10+(c-'A')
    end
    return 36+(c-'a')
end

function encode(io,b::Array{UInt8,1},i=0)
    while i<length(b)
        a=UInt128(0)
        for j=8:-1:1
            a=a*257
            if i+j<=length(b)
                a=a+1+b[i+j]
            end
        end
        i=i+8
        for j=1:11
            write(io,inttochar(mod(a,62)))
            a=div(a,62)
        end
    end
end

function encode(b::Array{UInt8,1})
    io=IOBuffer()
    encode(io,b)
    return String(take!(io))
end

function decode(s::AbstractString)
    b=Array{UInt8,1}()
    i=0
    while i<length(s)
        a=UInt128(0)
        for j=11:-1:1
            a=a*62+chartoint(s[i+j])
        end
        i=i+11
        for j=1:8
            x=mod(a,257)
            a=div(a,257)
            if x>0
                push!(b,UInt8(x-1))
            end
        end
    end
    return b
end

end
