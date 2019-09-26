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

using LRUCache
using DataFrames
const costhist = LRU{Tuple{DataFrames.DataFrame, Set{Symbol}}, Int}(maxsize = 1000)
function cost(records, selected)
    get!(costhist, (records, selected)) do
        selected = collect(selected)
        if length(selected) == 0
            ns = [size(records, 1)]
        else
            complete_records = dropmissing(records, selected)
            ns = [size(df, 1) for df in groupby(complete_records, selected)]
        end
        sum([n * (n - 1) / 2 for n in ns])
    end
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
