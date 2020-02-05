include("src/block.jl")
using ..Blocking
using ..Blocking.Conjunctions
using DataFrames
using Feather

recs = Feather.read("input/small-recs.feather")
pairs = Feather.read("input/small-pairs.feather")
budget = 5_000

problem = Blocking.Problem(
    recs,
    pairs,
    # array of candidate rules
    [n for n in names(recs) if n ∉ (:id, :recordid)],
    budget)

conj = Conjunctions.empty(problem)

function add_orig(conjunction)
    problem = conjunction.problem
    current = conjunction
    current_ratio = current.value / current.cost
    for rule in conjunction.unselected
        candidate = Conjunctions.and(conjunction, rule)
        candidate_ratio = candidate.value / candidate.cost
        candidate_ratio < current_ratio && continue
        if candidate_ratio > current_ratio
            current = candidate
            current_ratio = candidate_ratio
            continue
        end

        if candidate.value > current.value
            current = candidate
            current_ratio = candidate_ratio
        end
    end
    # if we end up with a solution that is too expensive, just add more
    # conjuncts until we have something acceptable
    if current.cost > problem.budget && !isempty(current.unselected)
        current == conjunction && return current
        print("   ...recursing...\n")
        return add_greedy(current)
    end
    return current
end

function add_multi(conjunction)
    problem = conjunction.problem
    init_ratio = conjunction.value / conjunction.cost
    current_ratios = [init_ratio for thread in 1:Threads.nthreads()]
    current_conjs  = [conjunction for thread in 1:Threads.nthreads()]
    all_rules = collect(conjunction.unselected)
    Threads.@threads for rule in all_rules
        candidate = Conjunctions.and(conjunction, rule)
        candidate_ratio = candidate.value / candidate.cost
        current_ratio = current_ratios[Threads.threadid()]
        current_value = current_conjs[Threads.threadid()].value
        candidate_ratio < current_ratio && continue
        if candidate_ratio > current_ratio
            current_conjs[Threads.threadid()] = candidate
            current_ratios[Threads.threadid()] = candidate_ratio
            continue
        end
        if candidate.value > current_value
            current_conjs[Threads.threadid()] = candidate
            current_ratios[Threads.threadid()] = candidate_ratio
        end
    end
    # if we end up with a solution that is too expensive, just add more
    # conjuncts until we have something acceptable
    maxval, maxindex = findmax(current_ratios)
    current = current_conjs[maxindex]
    if current.cost > problem.budget && !isempty(current.unselected)
        current == conjunction && return current
        print("   ...recursing...\n")
        return add_greedy(current)
    end
    return current
end

using BenchmarkTools
add_orig(conj).value
add_orig(conj).cost
add_multi(conj).value
add_multi(conj).cost
show(add_multi(conj))
show(add_orig(conj))

@benchmark add_orig(conj)
@benchmark add_multi(conj)
