module Blocking

using DataFrames

struct Problem
    records::DataFrame
    npairs::Int
    rules::Dict{Symbol, Array{Bool, 1}}
    budget::Int
    costcalcs::Dict{Set{Symbol}, Float64}
end

abstract type Solution end

# useful constructors
function rule_dict(matches, candidates)
    Dict(rule => matches[!, rule] for rule in candidates)
end

function Problem(records::DataFrame,
                 matches::DataFrame,
                 candidates::Array{Symbol, 1},
                 budget::Int)
    rules = rule_dict(matches, candidates)
    Blocking.Problem(records,
                     size(matches, 1,),
                     rules,
                     budget,
                     Dict{Set{Symbol}, Float64}())
end

function pairs(problem, selected)
    ismatch = repeat([true], problem.npairs)
    for rule in selected
        ismatch = ismatch .& problem.rules[rule]
    end
    return ismatch
end

rules(problem) = keys(problem.rules)

include("conjunction.jl")
include("search.jl")

end
