Some code to test out ideas about searching for blocking rules.

`write/output` describes the local search algorithm that I'm using.

`block/output` contains results -- that includes the final rule found for each simulated dataset along with numbers of true matches covered by the rule and the total number of pairs generated. There you can also find timestamps from each iteration, to get an idea of performance.

The goal is to find a disjunction of conjunctions that covers as many matching pairs as possible, constrained by a maximum `budget` of total pairs generated.

Each conjunction gives rise to an associated `subproblem`, that is, the original problem minus the pairs covered by the conjunction. The disjunction search is then just an iterative conjunction search -- we find a conjunction, we generate the subproblem, then find a conjunction for the subproblem, add it to our growing disjunction, and continue until we run out of `budget` or we've covered all matching pairs in the training data.

Conjunction search proceeds via local moves. A local move is either adding a single conjunct or removing a single conjunct from a conjunction. Starting from some initial position (such as the empty conjunction, the "full" conjunction of all atomic rules, a random conjunction, etc.), we make local moves based on some heuristic until we enounter a stopping criterion, and then emit the conjunction, and start a new conjunction search on the subproblem.

I've implemented two local search heuristics (block/src/search.jl):

- `add_conjunct` starts with the empty conjunction and, at each step, selects the conjunct whose addition would result in the highest ratio of `value/cost`. A conjuntion's `value` is the number of true pairs it covers, and the `cost` is the total number of pairs it would generate. We stop when we can no longer improve the ratio.
- Similarly, `drop_conjunct` starts with the full conjunction (every available rule), and greedily drops conjuncts.

Both of these heuristics involve evaluating every possible move at each step. Another possibility is to propose local moves one at a time, and select a move with probability proportional to the `value/cost` ratio . . ..
