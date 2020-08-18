include("block.jl")
using .Blocking
using .Blocking.Conjunctions
using .Blocking.Search

using Dates
using Feather

# start with an empty conjunction, and add conjuncts until some stopping pt
# pull out the resulting subproblem (remaining uncovered pairs, reduced budget)
# go again with the subproblem
# etc.
function disjunct_search(problem)
    res = Blocking.Solution[]
    while problem.npairs > 0
        solution = Blocking.Search.greedy(problem,
                                          Blocking.Conjunctions.empty(problem),
                                          Blocking.Search.add_greedy)
        solution.cost > problem.budget && break
        solution.value == 0 && break
        append!(res, [solution])
        problem = Blocking.Conjunctions.subproblem(solution)
    end
    return res
end

function emit(solution)
    recs = solution.problem.records
    cols = union(collect(solution.selected), [:recordid])
    blocks = filter(row -> row.recordid < row.recordid_1,
                    join(solution.problem.records[!, cols],
                         solution.problem.records[!, cols],
                         on = collect(solution.selected), makeunique = true))
    blocks[!, [:recordid, :recordid_1]]
end

# matches_file = "input/small-pairs.feather"
# records_file = "input/small-recs.feather"
# budget = 5000
function main()
    records_file = ARGS[1]
    matches_file = ARGS[2]
    budget       = parse(Int, ARGS[3])

    records = Feather.read(records_file)
    matches = Feather.read(matches_file)

    candidates = [n for n in propertynames(records) if n ∉ (:id, :recordid)]
    problem = Blocking.Problem(records, matches, candidates, budget)

    start_time = now()
    my_solution = disjunct_search(problem);
    end_time = now()
    all_pairs = reduce((a,b) -> [a;b], [emit(solution) for solution in my_solution])
    unique_pairs = unique(all_pairs)
    covered = join(unique_pairs, matches,
                   on = [Pair(:recordid, :recordid_1),
                         Pair(:recordid_1, :recordid_2)],
                   kind = :inner)

    println("Original problem")
    println("================")
    println("records: ", size(problem.records, 1))
    println("pairs  : ", problem.npairs)
    println("columns: ", length(problem.rules))
    println()

    println("Performance")
    println("===========")
    println("Threads      : ", Threads.nthreads())
    println("Rules visited: ", length(problem.costcalcs))
    println("Time         : ",
            Dates.canonicalize(Dates.CompoundPeriod(end_time - start_time)))
    println()

    println("Quality")
    println("=======")
    println("Total pairs (expected): ", sum(s.cost for s in my_solution))
    println("              (actual): ", size(all_pairs, 1))
    println("      (actual-deduped): ", size(unique!(all_pairs), 1))
    println("Coverage    (expected): ", sum(s.value for s in my_solution))
    println("              (actual): ", size(covered, 1))
    println()

    println("Final rule")
    println("==========")
    println(join(["("*show(solution)*")" for solution in my_solution], " ∨\n"))
end

main()

# done.
