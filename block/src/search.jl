module Search

using ..Blocking
using ..Conjunctions

function add_greedy(conjunction)
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
    while next != current && length(next.selected) <= 3
        current = next
        next = advance(current)
    end
    return next
end

end
