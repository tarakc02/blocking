module Search

using ..Blocking
using ..Conjunctions

function add_greedy(conjunction)
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

        if candidate.value > current.value || candidate.cost < current.cost
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

function drop_greedy(conjunction)
    current = conjunction
    for rule in conjunction.selected
        candidate = Conjunctions.butnot(conjunction, rule)
        candidate.cost > candidate.problem.budget && continue

        candidate.value < current.value && continue
        if candidate.value > current.value
            current = candidate
            continue
        end

        if candidate.cost < current.cost
            current = candidate
        end
    end
    return current
end

function greedy(problem, start, advance)
    current = start
    next = advance(current)
    while next != current
        current = next
        next = advance(current)
    end
    return next
end

end
