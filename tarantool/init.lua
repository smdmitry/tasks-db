-- Tarantool с дисковым движком vinyl (LSM). Память движка урезана через VINYL_MEMORY / VINYL_CACHE,
-- остальное (page cache файлов) ограничено лимитом контейнера.
box.cfg{
    listen = 3301,
    work_dir = '/var/lib/tarantool',
    memtx_memory = 64 * 1024 * 1024,
    vinyl_memory = tonumber(os.getenv('VINYL_MEMORY')) or 256 * 1024 * 1024,
    vinyl_cache = tonumber(os.getenv('VINYL_CACHE')) or 128 * 1024 * 1024,
    vinyl_write_threads = 4,
    vinyl_read_threads = 2,
    wal_mode = 'write', -- без fsync на каждый коммит, как commitlog у Scylla/Cassandra
    checkpoint_interval = 600,
    net_msg_max = 8192,
    readahead = 1024 * 1024,
    log_level = 5,
}

box.schema.user.grant('guest', 'read,write,execute,create,drop', 'universe', nil, {if_not_exists = true})

local format = {
    {name = 'hour', type = 'unsigned'},
    {name = 'minute', type = 'unsigned'},
    {name = 'shard', type = 'unsigned'},
    {name = 'id', type = 'unsigned'},
    {name = 'uid', type = 'unsigned'},
    {name = 'payload', type = 'string'}, -- go-tarantool шлёт []byte как msgpack str
}

-- ensure создаёт vinyl-спейс с PK (hour, minute, shard, id): бакет — непрерывный диапазон ключа.
function ensure(name)
    if box.space[name] ~= nil then return end
    local s = box.schema.space.create(name, {engine = 'vinyl', format = format, if_not_exists = true})
    s:create_index('pk', {parts = {'hour', 'minute', 'shard', 'id'}, if_not_exists = true})
end

function reset()
    local names = {}
    for _, s in box.space._space:pairs() do
        local sp = box.space[s[3]]
        if sp ~= nil and sp.engine == 'vinyl' then names[#names + 1] = s[3] end
    end
    for _, n in ipairs(names) do box.space[n]:drop() end
end

function put(name, t)
    box.space[name]:replace(t)
end

-- put_batch: {name = {tuple, ...}, ...} одной транзакцией
function put_batch(groups)
    box.begin()
    for name, ts in pairs(groups) do
        local s = box.space[name]
        for _, t in ipairs(ts) do s:replace(t) end
    end
    box.commit()
end

-- page: keyset-чтение бакета после id=last, без удаления
function page(name, h, m, sh, last, limit)
    local s = box.space[name]
    local out = {}
    if s == nil then return out end
    for _, t in s.index.pk:pairs({h, m, sh, last}, {iterator = 'GT'}) do
        if t[1] ~= h or t[2] ~= m or t[3] ~= sh then break end
        out[#out + 1] = {t[4], t[6]}
        if #out >= limit then break end
    end
    return out
end

-- take: прочитать страницу бакета и удалить её в той же транзакции (очередь с удалением строк)
function take(name, h, m, sh, limit)
    local s = box.space[name]
    local out = {}
    box.begin()
    for _, t in s.index.pk:pairs({h, m, sh}, {iterator = 'EQ'}) do
        out[#out + 1] = {t[4], t[6]}
        if #out >= limit then break end
    end
    for _, r in ipairs(out) do s:delete({h, m, sh, r[1]}) end
    box.commit()
    return out
end

function drop(name)
    if box.space[name] ~= nil then box.space[name]:drop() end
end

function stats()
    local disk, rows = 0, 0
    for _, s in box.space._space:pairs() do
        local sp = box.space[s[3]]
        if sp ~= nil and sp.engine == 'vinyl' then
            local st = sp.index.pk:stat()
            disk = disk + st.disk.bytes
            rows = rows + st.rows
        end
    end
    return {disk, box.stat.vinyl().memory.level0, rows}
end

local f = io.open('/tmp/ready', 'w')
f:write('ok')
f:close()
