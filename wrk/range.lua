math.randomseed(os.time())

function generateRange()
    key = math.random(1, 9)
    return key, key + 1
end

request = function()
    from, to = generateRange()
    path = "/v0/entities?start=key" .. from .. "&end=key" .. to
    wrk.method = "GET"
    return wrk.format(nil, path)
end
