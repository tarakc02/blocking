module Blocking

using DataFrames

struct Problem
    records::DataFrame
    npairs::Int
    rules::Dict{Symbol, Array{Bool, 1}}
end

abstract type Solution end
struct Conjunction <: Solution
    problem::Problem
    selected::Array{Symbol, 1}
    candidates::Array{Symbol, 1}
    cost::Int
    reward::Int
end

# useful constructors
function rule_dict(matches, candidates)
    Dict(rule => matches[!, rule] for rule in candidates)
end

function Problem(records::DataFrame,
                 matches::DataFrame,
                 candidates::Array{Symbol, 1})
    rules = rule_dict(matches, candidates)
    Blocking.Problem(records, size(matches, 1), rules)
end

function Conjunction(problem, selected, candidates)
    c = cost(problem, selected)
    r = reward(problem, selected)
    Conjunction(problem, selected, candidates, c, r)
end

function reward(problem, selected)
    ismatch = repeat([true], problem.npairs)
    for rule in selected
        ismatch = ismatch .& problem.rules[rule]
    end
    sum(ismatch)
end

function cost(problem, selected)
    if length(selected) == 0
        ns = [size(problem.records, 1)]
    else
        complete_records = dropmissing(problem.records, selected)
        ns = [size(df, 1) for df in groupby(complete_records, selected)]
    end
    sum([n * (n - 1) / 2 for n in ns])
end

function Base.show(solution::Conjunction)
    isempty(solution.selected) && return ""
    reduce((r1, r2) -> r1 *" ∧ "* r2,
           map(String, solution.selected))
end

# should be able to guarantee that rule ∈ conj1.candidates
function and(conj1::Conjunction, rule::Symbol)
    Conjunction(conj1.problem,
                union(conj1.selected, [rule]),
                setdiff(conj1.candidates, [rule]))
end

# should be able to guarantee that rule ∈ conj1.selected
function unand(conj1::Conjunction, rule::Symbol)
    Conjunction(conj1.problem,
                setdiff(conj1.selected, [rule]),
                conj1.candidates)
end

# assert !isempty(conj.candidates)
function add_random(conj::Conjunction)
    new_rule = rand(conj.candidates, 1)
    and(conj, new_rule[1])
end

# assert !isempty(conj.selected)
function drop_random(conj::Conjunction)
    to_drop = rand(conj.selected, 1)
    unand(conj, to_drop[1])
end

end
