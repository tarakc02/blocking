module Conjunctions

using ..Blocking

struct Conjunction <: Blocking.Solution
    problem::Blocking.Problem
    selected::Set{Symbol}
    unselected::Set{Symbol}
    cost::Int
    value::Int
end

function Conjunction(problem::Blocking.Problem, selected, unselected)
    cst = cost(problem.records, selected)
    val = value(problem, selected)
    Conjunction(problem, selected, unselected, cst, val)
end

function full(problem::Blocking.Problem)
    selected = Set(keys(problem.rules))
    Conjunction(problem, selected, Set{Symbol}())
end

function empty(problem::Blocking.Problem)
    selected = Set(keys(problem.rules))
    Conjunction(problem, Set{Symbol}(), selected)
end

function value(problem, selected)
    ismatch = repeat([true], problem.npairs)
    for rule in selected
        ismatch = ismatch .& problem.rules[rule]
    end
    sum(ismatch)
end

function update_dict(key, dict)
    if haskey(dict, key)
        dict[key] += 1
    else
        dict[key] = 1
    end
end

function cost(records, selected,
              group_sizes = [Dict{UInt64, Float64}() for d in 1:Threads.nthreads()])
    if length(selected) == 0
        n_recs = size(records, 1)
        return n_recs * (n_recs - 1) / 2
    end
    selected = collect(selected)
    @inbounds Threads.@threads for row = 1:size(records, 1)
        key = hash(records[row, selected])
        update_dict(key, group_sizes[Threads.threadid()])
    end
    gs = merge(+, group_sizes...)
    sum(size * (size - 1) / 2 for (key, size) in gs)
end


function Base.show(solution::Conjunction)
    isempty(solution.selected) && return ""
    reduce((r1, r2) -> r1 *" ∧ "* r2,
           map(String, collect(solution.selected)))
end

# should be able to guarantee that rule ∈ problem.rules
function and(conj::Conjunction, rule::Symbol)
    Conjunction(conj.problem,
                union(conj.selected, [rule]),
                setdiff(conj.unselected, [rule]))
end

# should be able to guarantee that rule ∈ conj1.selected
function butnot(conj::Conjunction, rule::Symbol)
    Conjunction(conj.problem,
                setdiff(conj.selected, [rule]),
                union(conj.unselected, [rule]))
end

function filter_rule(rule, remove)
    keep_indices = [i for i in 1:length(remove) if !remove[i]]
    [rule[k] for k in keep_indices]
end

function subproblem(conjunction)
    problem = conjunction.problem
    recs = problem.records
    npairs = problem.npairs - conjunction.value
    budget = problem.budget - conjunction.cost

    remove = Blocking.pairs(conjunction.problem, conjunction.selected)
    rules = Dict(rule => filter_rule(problem.rules[rule], remove) for rule in keys(problem.rules))
    Blocking.Problem(recs, npairs, rules, budget)
end

end
