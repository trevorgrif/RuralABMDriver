using Graphs
using SimpleWeightedGraphs
using StatsBase
using DuckDB, DataFrames
using Random
using Plots
using PlotlyJS
using Distributions

#====================#
# Connection Methods #
#====================#

function run_query(query, connection)
    DBInterface.execute(connection, query)
end

function create_con()
    return DBInterface.connect(DuckDB.DB, "data/GDWLND.duckdb")
end

function close_con(connection)
    DBInterface.close(connection)
end

#====================#
# Distance Functions #
#====================#

"""
    geometric_mean(v)

Compute the geometric mean of all vectors
"""
function geometric_mean(v)
    return sqrt.(prod.(v))
end

"""
    arithmetic_mean(v)

Compute the arithmetic mean of all vectors
"""
function arithmetic_mean(v)
    return mean.(v)
end

#==================================#
# Accessing Upper Half of a Matrix #
#==================================#

function get_ArithmeticKeyPoints(n)
    KeyPoints = []
    for i in 1:n
        append!(KeyPoints, get_S(n,i))
    end
    return KeyPoints
end

function get_S(n,i)
    return Int((i-1)*(n+1-(i/2.0)))
end

"""
    get_upper_half(matrix)

Get the upper half of a matrix as a vector.
"""
function get_upper_half(matrix)
    n = size(matrix)[1]
    vector = []
    for i in 1:n-1
        for j in i+1:n
            push!(vector, matrix[i,j])
        end
    end
    return vector
end

"""
    convert_to_vector(List)

Convert a string of comma separated values to a vector of Int64.
"""
function convert_to_vector(List)
    return parse.(Int64, split(List, ","))
end

"""
    social_contact_matrix_to_graph(EpidemicID::Int64)

Create a simple weighted graph from a social contact matrix.
"""
function social_contact_matrix_to_graph(EpidemicID::Int64)
    # Compute the TownID correlated to the EpidemicID
    query = """
        SELECT TownDim.TownID
        FROM EpidemicLoad
        JOIN EpidemicDim on EpidemicLoad.EpidemicID = EpidemicDim.EpidemicID
        JOIN BehaviorDim on EpidemicDim.BehaviorID = BehaviorDim.BehaviorID
        JOIN NetworkDim on BehaviorDim.NetworkID = NetworkDim.NetworkID
        JOIN TownDim on TownDim.TownID = NetworkDim.TownID
        WHERE EpidemicLoad.EpidemicID = $EpidemicID
    """
    TownID = run_query(query, create_con()) |> DataFrame
    TownID = TownID[1,1] |> Int64

    # Query the population size (Need to add to DataBase)
    PopulationSize = 386
    
    # Load the SCM
    query = """
        SELECT * 
        FROM EpidemicSCMLoad_$TownID
        WHERE EpidemicID = $EpidemicID
    """
    SCMCompact = run_query(query, create_con()) |> DataFrame
    SCM = SCMCompact[1,2] |> convert_to_vector

    # Create a simple weighted graph
    g = SimpleWeightedGraph(PopulationSize)
    
    # Loop over all nodes
    KeyPoints = get_ArithmeticKeyPoints(PopulationSize-1)
    KeyPointsItr = 1
    for i in 1:PopulationSize-1
        # Loop over all nodes
        for j in i+1:PopulationSize
            # Add an edge between node i and node j with weight matrix[i, j]
            weight = SCM[KeyPoints[KeyPointsItr]+j-i]
            weight <= 2 && continue
            add_edge!(g, i, j, weight)
        end
        KeyPointsItr += 1
    end
    return g
end


"""
    social_contact_matrix_to_graph_norm(EpidemicID::Int64)

Create a simple weighted graph from a social contact matrix.
"""
function social_contact_matrix_to_normalized_graph(EpidemicID::Int64)
    # Compute the TownID correlated to the EpidemicID
    query = """
        SELECT TownDim.TownID
        FROM EpidemicLoad
        JOIN EpidemicDim on EpidemicLoad.EpidemicID = EpidemicDim.EpidemicID
        JOIN BehaviorDim on EpidemicDim.BehaviorID = BehaviorDim.BehaviorID
        JOIN NetworkDim on BehaviorDim.NetworkID = NetworkDim.NetworkID
        JOIN TownDim on TownDim.TownID = NetworkDim.TownID
        WHERE EpidemicLoad.EpidemicID = $EpidemicID
    """
    TownID = run_query(query, create_con()) |> DataFrame
    TownID = TownID[1,1] |> Int64

    # Query the population size (Need to add to DataBase)
    PopulationSize = 386
    
    # Load the SCM
    query = """
        SELECT * 
        FROM EpidemicSCMLoad_$TownID
        WHERE EpidemicID = $EpidemicID
    """
    SCMCompact = run_query(query, create_con()) |> DataFrame
    SCM = SCMCompact[1,2] |> convert_to_vector

    # Create a simple weighted graph
    g = SimpleWeightedGraph(PopulationSize)
    MaxWeight = maximum(SCM)
    
    # Loop over all nodes
    KeyPoints = get_ArithmeticKeyPoints(PopulationSize-1)
    KeyPointsItr = 1
    for i in 1:PopulationSize-1
        # Loop over all nodes
        for j in i+1:PopulationSize
            # Add an edge between node i and node j with weight matrix[i, j]
            weight = SCM[KeyPoints[KeyPointsItr]+j-i]
            weight <= 2 && continue # Ignore non-connected verticies

            # Normalize the weight (maybe filter out low weights)
            weight = (MaxWeight+1) - weight

            add_edge!(g, i, j, weight)
        end
        KeyPointsItr += 1
    end
    return g
end

"""

Iterate over all triples of the graph G
"""
function triplets(g::SimpleWeightedGraph)
    # Create a vector to store the triplets
    closed_triplet_weights = []
    triplet_weights = []

    # Loop over all nodes
    for i in 1:nv(g)
        # Get the neighbors of node i
        neighbors = Graphs.neighbors(g, i)
        # Get the number of neighbors
        n = length(neighbors)
        # If the node has less than 2 neighbors, the clustering coefficient is 0
        if n < 2
            continue
        else
            # Get the number of edges between the neighbors of node i
            for j in 1:n
                for k in j+1:n
                    # Triplet detected
                    weight_1 = g.weights[i, neighbors[j]]
                    weight_2 = g.weights[i, neighbors[k]]
                    push!(triplet_weights, [weight_1, weight_2])

                    # Check for triangle
                    if has_edge(g, neighbors[j], neighbors[k])
                        push!(closed_triplet_weights, [weight_1, weight_2]) 
                    end
                end
            end
        end
    end
    return Dict("triplet_weights" => triplet_weights, "closed_triplet_weights" => closed_triplet_weights)
end


"""
clustering_coef(g::SimpleWeightedGraph; method::Function = geometric_mean)

Compute the global clustering coefficient of a weighted graph. Method can be any function that takes a vector of vectors as input and returns a vector of the same length.

See method::Functino = arithmetic_mean

"""
function global_clustering_coefficient(g::SimpleWeightedGraph; method::Function = geometric_mean)
    weights = triplets(g)
    return sum(method(weights["closed_triplet_weights"])) / sum(method(weights["triplet_weights"]))
end

"""
    compute_small_world_statistic(g::SimpleWeightedGraph)
"""
function compute_small_world_statistic(EpidemicID::Int64; β::Float64 = 1.0)
    # We work with a normalized version of g where the edge weight = MaxWeight + 1 - weight
    # This way the shortest path is the path with the strongest connections
    # Load the Graph
    g = social_contact_matrix_to_graph(EpidemicID)
    g_normed = social_contact_matrix_to_normalized_graph(EpidemicID)

    # Compute the clustering coefficient and the average shortest path for g
    clustering_coefficient = global_clustering_coefficient(g)
    average_shortest_path = floyd_warshall_shortest_paths(g_normed).dists |> get_upper_half |> mean

    # Create a randomized graph with the same number of nodes and edges as g
    average_degree = (sum(degree(g)) / length(degree(g))) |> floor |> Int
    weight_distribution, weights = get_weight_distribution(g)
    weight_normed_distribution, weights_normed = get_weight_distribution(g_normed)

    g_random = watts_strogatz(nv(g), average_degree, β, weight_distribution, weights)
    g_random_normed = watts_strogatz(nv(g_normed), average_degree, β, weight_normed_distribution, weights_normed)

    # Compute the clustering coefficient and the average shortest path for the randomized graph
    random_clustering_coefficient = global_clustering_coefficient(g_random)
    random_average_shortest_path = floyd_warshall_shortest_paths(g_random_normed).dists |> get_upper_half |> mean

    return Dict(
        "global_clustering_coefficient" => clustering_coefficient, 
        "random_global_clustering_coefficient" => random_clustering_coefficient,
        "average_shortest_path" => average_shortest_path,
        "random_average_shortest_path" => random_average_shortest_path
        )
end

# Custom Watts Strogatz graph
function watts_strogatz(
    n::Integer,
    k::Integer,
    β::Real,
    weight_distribution,
    weights;
    is_directed::Bool=false,
    remove_edges::Bool=true,
    rng::Union{Nothing,AbstractRNG}=nothing,
    seed::Union{Nothing,Integer}=nothing,
)
    @assert k < n

    g = SimpleWeightedGraph(n)
    # The ith next vertex, in clockwise order.
    # (Reduce to zero-based indexing, so the modulo works, by subtracting 1
    # before and adding 1 after.)
    @inline target(s, i) = ((s + i - 1) % n) + 1

    # Phase 1: For each step size i, add an edge from each vertex s to the ith
    # next vertex, in clockwise order.

    for i in 1:div(k, 2), s in 1:n
        add_edge!(g, s, target(s, i), weights[findall(!iszero, rand(weight_distribution))[1]])
    end

    # Phase 2: For each step size i and each vertex s, consider the edge to the
    # ith next vertex, in clockwise order. With probability β, delete the edge
    # and rewire it to any (valid) target, chosen uniformly at random.

    rng = Graphs.rng_from_rng_or_seed(rng, seed)
    for i in 1:div(k, 2), s in 1:n

        # We only rewire with a probability β, and we only worry about rewiring
        # if there is some vertex not connected to s; otherwise, the only valid
        # rewiring is to reconnect to the ith next vertex, and there is no work
        # to do.
        (rand(rng) < β && degree(g, s) < n - 1) || continue

        t = target(s, i)

        while true
            d = rand(rng, 1:n)          # Tentative new target
            d == s && continue          # Self-loops prohibited
            d == t && break             # Rewired to original target

            t_w = get_weight(g, s, t)   # Current connection
            d_w = get_weight(g, s, d)   # Potential new connection
            
            d_w != 0.0 && continue          # Already connected

            if add_edge!(g, s, d, t_w)       # Always returns true for SimpleWeightedGraph
                remove_edges && rem_edge!(g, s, t)     # True rewiring: Delete original edge
                break                                   # We found a valid target
            end
        end
    end
    return g
end

#===========================================#
# Re-creating the Graph from Watts-Strogatz #
#===========================================#

"""
    recreate_smallworld_graph(n = 20, k = 4; weight = 0; sample_size = 10)

Recreates the Watts-Strogatz graph from the original paper with n vertices, k nearest neighbors, and rewiring probability p. 

Set weight to a value other than 0 to create a weighted graph.
"""
function recreate_smallworld_graph(n = 20, k = 4; weight_distribution = Multinomial(1,[1.0]), weights = 0, sample_size = 10)
    plotlyjs()

    if weights == 0
        global_clustering_coefficient_function = Graphs.global_clustering_coefficient
        watts_strogatz_function = Graphs.watts_strogatz
        args = (n, k, 0)
    else
        global_clustering_coefficient_function = global_clustering_coefficient
        watts_strogatz_function = watts_strogatz
        args = (n, k, 0, weight_distribution, weights)
    end

    avg_C = zeros(15)
    avg_L = zeros(15)

    # Generate p-values
    p_values = []
    for i in 1:15
        p = i/15.0
        push!(p_values, p)
    end
    p_values = p_values .- 1
    p_values = p_values .* 4
    p_values = 10 .^ (p_values)
    return p_values
    
    ring_lattice = watts_strogatz_function(args...)
    
    C_0 = global_clustering_coefficient_function(ring_lattice)
    L_0 = floyd_warshall_shortest_paths(ring_lattice).dists |> get_upper_half |> mean

    for i in 1:sample_size
        # Generate a random graph for each p-value
        random_graphs = []
        for p in p_values
            if weights == 0
                args = (n, k, p)
            else
                args = (n, k, p, weight_distribution, weights)                
            end
            g = watts_strogatz_function(args...)

            push!(random_graphs, g)
        end

        # Compute L(p) and C(p) for each graph
        L_p = []
        C_p = []
        for g in random_graphs
            L = floyd_warshall_shortest_paths(g).dists |> get_upper_half |> mean
            C = global_clustering_coefficient_function(g)
            push!(L_p, L)
            push!(C_p, C)
        end

        # Divide each C(p) value by C(0)
        C_p = C_p ./ C_0

        # Divide each L(p) value by L(0)
        L_p = L_p ./ L_0

        avg_C .+= C_p
        avg_L .+= L_p
    end

    avg_C = avg_C ./ sample_size
    avg_L = avg_L ./ sample_size
    
    # Graph C_p and L_p as two scatter plots 
    if weight == 0
        title = "Non-Weighted Graphs"
    else
        title = "Weighted Graphs"
    end
    x_values = [0.0005, 0.0006, 0.0007, 0.0008, 0.0009, 0.001, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    x_display_values = [" ", " ", " ", " ", " ", "0.001", " ", " ", " ", " ", " ", "0.01", " ", " ", " ", " ", " ", "0.1", " ", " ", " ", " ", "1.0"]

    Plots.plot(
        p_values, 
        avg_C, 
        xscale=:log10, 
        label="C(p)/C(0)", 
        xlabel="p", 
        ylabel="Ratio over Regular Lattice",
        seriestype=:scatter
        )
    Plots.scatter!(
        p_values, 
        avg_L,
        xscale=:log10,
        label="L(p)/L(0)", 
        xlabel="p", 
        ylabel="Ratio over Regular Lattice", 
        title=title,
        xticks=(x_values,x_display_values)
        )
end

# Get the weight distribution of a graph G
function get_weight_distribution(G)
    weights = []
    for e in edges(G)
        push!(weights, e.weight)
    end
    aggWeights = countmap(weights)
    total = sum(values(aggWeights))

    probability_vector = []
    for w in keys(aggWeights)
        push!(probability_vector, aggWeights[w]/total)
    end
    probability_vector = convert.(Float64, probability_vector)

    return Multinomial(1, probability_vector), collect(keys(aggWeights))
end