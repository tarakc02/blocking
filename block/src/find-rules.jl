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
        print("time              : ", Dates.format(now(), "HH:MM:SS"), "\n")
        print("remaining budget  : ", problem.budget, "\n")
        print("remaining pairs   : ", problem.npairs, "\n")
        solution = Blocking.Search.greedy(problem,
                                          Blocking.Conjunctions.empty(problem),
                                          Blocking.Search.add_greedy)
        solution.cost > problem.budget && break
        solution.value == 0 && break
        print("FOUND CONJUNCTION : ", show(solution), "\n")
        print("             cost : ", solution.cost, "\n")
        print("            value : ", solution.value, "\n\n")
        append!(res, [solution])
        problem = Blocking.Conjunctions.subproblem(solution)
    end
    return res
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

    candidates = [n for n in names(records) if n ∉ (:id, :recordid)]
    problem = Blocking.Problem(records, matches, candidates, budget)

    my_solution = disjunct_search(problem);
    tot_value = sum([s.value for s in my_solution])
    tot_cost  = sum([s.cost  for s in my_solution])
    print("\nDone.\n")
    print("time              : ", Dates.format(now(), "HH:MM:SS"), "\n")
    print("matches found  : ", tot_value, " (out of ", problem.npairs, ")", "\n")
    print("pairs generated: ", tot_cost, "\n")
    print("distinct conjunctions evaluated: ", length(problem.costcalcs), "\n")
    print("Final rule:\n")
    print(join(["("*show(solution)*")" for solution in my_solution], " ∨\n"), "\n")
end

main()

#function emit(solution)
#    recs = solution.problem.records
#    cols = union(collect(solution.selected), [:recordid])
#    blocks = filter(row -> row.recordid < row.recordid_1,
#                    join(solution.problem.records[!, cols],
#                         solution.problem.records[!, cols],
#                         on = collect(solution.selected), makeunique = true))
#    blocks[!, [:recordid, :recordid_1]]
#end
#
#function check(result, matches)
#    matching_pairs = matches[!, [:recordid_1, :recordid_2]]
#
#    all_pairs = reduce((a,b) -> [a;b], [emit(solution) for solution in result])
#    unique!(join(all_pairs, matching_pairs,
#                 on = [(:recordid, :recordid_1), (:recordid_1, :recordid_2)], kind = :inner))
#end

# done.
