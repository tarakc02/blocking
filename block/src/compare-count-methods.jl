using StatsBase
using DataFrames
using Feather
using BenchmarkTools

recs = Feather.read("input/medium-recs.feather")
candidates = [n for n in names(recs) if n ∉ (:id, :recordid)]
testsel1 = Set(sample(candidates, 1, replace = false))
testsel2 = Set(sample(candidates, 2, replace = false))
testsel3 = Set(sample(candidates, 3, replace = false))
testsel8 = Set(sample(candidates, 8, replace = false))

function df_groupby(records, selected)
    selected = collect(selected)
    if length(selected) == 0
        ns = [size(records, 1)]
    else
        complete_records = dropmissing(records, selected)
        ns = [size(df, 1) for df in groupby(complete_records, selected)]
    end
    sum([n * (n - 1) / 2 for n in ns])
end

function update_dict(key, dict)
    if haskey(dict, key)
        dict[key] += 1
    else
        dict[key] = 1
    end
end

function merge_dicts(a, b)
    if length(a) > length(b)
        main = a
        side = b
    else
        main = b
        side = a
    end
    for (key, value) in side
        if haskey(main, key)
            main[key] += side[key]
        else
            main[key] = side[key]
        end
    end
    return main
end

function rmerge(dicts)
    l = length(dicts)
    l == 1 && return dicts[1]
    l == 2 && return merge_dicts(dicts[1], dicts[2])
    rest = Threads.@spawn rmerge(dicts[3:l])
    first_two = merge_dicts(dicts[1], dicts[2])
    merge_dicts(first_two, fetch(rest))
end

function forloop_1thread(records, selected)
    selected = collect(selected)
    group_sizes = Dict{UInt64, Float64}()
    @inbounds for i = 1:size(records, 1)
        key = hash(records[i, selected])
        update_dict(key, group_sizes)
    end
    sum(size * (size - 1) / 2 for (key, size) in group_sizes)
end

function forloop_nthread(records, selected,
                         group_sizes = [Dict{UInt64, Float64}() for d in 1:Threads.nthreads()])
    selected = collect(selected)
    @inbounds Threads.@threads for row = 1:size(records, 1)
        key = hash(records[row, selected])
        update_dict(key, group_sizes[Threads.threadid()])
    end
    gs = reduce(merge_dicts, group_sizes)
    sum(size * (size - 1) / 2 for (key, size) in gs)
end

function forloop_nthreadx(records, selected,
                         group_sizes = [Dict{UInt64, Float64}() for d in 1:Threads.nthreads()])
    selected = collect(selected)
    @inbounds Threads.@threads for row = 1:size(records, 1)
        key = hash(records[row, selected])
        update_dict(key, group_sizes[Threads.threadid()])
    end
    gs = rmerge(group_sizes)
    sum(size * (size - 1) / 2 for (key, size) in gs)
end

@benchmark df_groupby(recs, testsel1)
@benchmark forloop_1thread(recs, testsel1)
@benchmark forloop_nthread(recs, testsel1)
@benchmark forloop_nthreadx(recs, testsel1)

@benchmark df_groupby(recs, testsel8)
@benchmark forloop_1thread(recs, testsel8)
@benchmark forloop_nthread(recs, testsel8)
@benchmark forloop_nthreadx(recs, testsel8)

@benchmark df_groupby(recs, testsel3)
@benchmark forloop_1thread(recs, testsel3)
@benchmark forloop_nthread(recs, testsel3)
@benchmark forloop_nthreadx(recs, testsel3)
