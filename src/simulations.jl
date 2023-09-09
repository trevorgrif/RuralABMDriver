
""" 
    run_ruralABM()

Run the RuralABM package with default parameters.

# Arguments
- `SOCIAL_NETWORKS=10`: Multiplicity of creating town social networks (Range: 1 -> infty).
- `NETWORK_LENGTH=30`: Length in days the model will be run to generate a social network (Range: 1 -> infty).
- `MASKING_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "2" generates [0,50] (Range: 0 -> 100).
- `VACCINATION_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "4" generates [0,25,50,75] (Range: 0 -> 100).
- `MODEL_RUNS=100`: Multiplicity model runs with disease spread (Range: 1 -> infty).
- `TOWN_NAMES=["Dubois"]`: Towns which will be run. Ensure input data exist for target towns.
"""
function _run_ruralABM(;
    SOCIAL_NETWORKS::Int = 10,
    NETWORK_LENGTH::Int = 30,
    MASKING_LEVELS::Int = 5,
    VACCINATION_LEVELS::Int = 5,
    DISTRIBUTION_TYPE = [0, 0],
    MODEL_RUNS::Int = 100,
    TOWN_NAMES = "small",
    STORE_NETWORK_SCM = true,
    STORE_EPIDEMIC_SCM = true,
    NUMBER_WORKERS = 5
    )
    # Verify input parameters
    @assert SOCIAL_NETWORKS > 0 "SOCIAL_NETWORKS must be greater than 0"
    @assert NETWORK_LENGTH >= 0 "NETWORK_LENGTH must be greater than or equal to 0"
    NETWORK_LENGTH == 0 && @warn "NETWORK_LENGTH is 0, social networks will not form before disease spread"
    @assert 1 <= MASKING_LEVELS <= 100 "MASKING_LEVELS must be greater than or equal to 0 and less than or equal to 100"
    @assert 1 <= VACCINATION_LEVELS <= 100 "VACCINATION_LEVELS must be greater than or equal to 0 and less than or equal to 100"
    @assert DISTRIBUTION_TYPE[1] in [0, 1] "DISTRIBUTION_TYPE[1] must be 0 or 1"
    @assert DISTRIBUTION_TYPE[2] in [0, 1] "DISTRIBUTION_TYPE[2] must be 0 or 1"
    @assert MODEL_RUNS > 0 "MODEL_RUNS must be greater than 0"

    # Check for database existence
    if !isfile(joinpath("data", "GDWLND.duckdb"))
        println("Creating Database Structure")
        create_database_structure()
    end

    # Run simulations in parallel
    println("Starting Simulation")
    _begin_simulations_faster(SOCIAL_NETWORKS, MASKING_LEVELS, VACCINATION_LEVELS, DISTRIBUTION_TYPE, MODEL_RUNS, NETWORK_LENGTH, TOWN_NAMES, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, NUMBER_WORKERS)

    # Vacuum database
    println("Vacuuming Database")
    _vacuum_database()

end

function _create_town!(town_type::String, connection::DuckDB.DB)
    # TODO: Validate with database instead of hard coding
    if town_type == "small" 
        PopulationID = 1
    elseif town_type == "large"
        PopulationID = 2
    end

    # Construct town and assign model meta-data
    model = Construct_Town(joinpath("lib", "RuralABM", "data", "example_towns", "$(town_type)_town", "population.csv"), joinpath("lib", "RuralABM", "data", "example_towns", "$(town_type)_town", "businesses.csv"))
    model.population_id = PopulationID

    # Get next Town ID
    query = """SELECT nextval('TownDimSequence')"""
    townID = _run_query(query, connection)[1,1]
    model.town_id = townID

    # Populate TownDim
    appender = DuckDB.Appender(connection, "TownDim")
    DuckDB.append(appender, model.town_id)
    DuckDB.append(appender, model.population_id)
    DuckDB.append(appender, length(model.business))
    DuckDB.append(appender, length(model.houses))
    DuckDB.append(appender, length(model.school))
    DuckDB.append(appender, length(model.daycare))
    DuckDB.append(appender, length(model.community_gathering))
    DuckDB.append(appender, model.number_adults)
    DuckDB.append(appender, model.number_elders)
    DuckDB.append(appender, model.number_children)
    DuckDB.append(appender, model.number_empty_businesses)
    DuckDB.append(appender, join(Serialize_Model(model), ","))
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Populate BusinessLoad
    appender = DuckDB.Appender(connection, "BusinessLoad")
    for row in eachrow(model.business_structure_dataframe)
        DuckDB.append(appender, model.town_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Populate HouseholdLoad
    appender = DuckDB.Appender(connection, "HouseholdLoad")
    for row in eachrow(model.household_structure_dataframe)
        DuckDB.append(appender, model.town_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    return model.town_id
end

function _create_network!(town_id::Int, duration::Int, connection::DuckDB.DB; STORE_NETWORK_SCM = false)
    # Get the model for the town_id
    model = _get_model_by_town_id(town_id, connection)
    model === nothing && return false

    # Run the model for duration number of days
    Run_Model!(model, duration=duration)

    # Store Results
    query = "SELECT nextval('NetworkDimSequence')"
    networkId = _run_query(query, connection)[1,1]
    model.network_id = networkId
    model.network_construction_length = duration
    
    # Append NetworkDim data
    appender = DuckDB.Appender(connection, "NetworkDim")
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.town_id)
    DuckDB.append(appender, model.network_construction_length)
    DuckDB.append(appender, join(Serialize_Model(model), ","))
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append NetworkSCMLoad data
    if STORE_NETWORK_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "NetworkSCMLoad")

        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end

    return model.network_id
end

function _create_network!(model, duration::Int, connection::DuckDB.DB; STORE_NETWORK_SCM = false)
    # Run the model for duration number of days
    Run_Model!(model, duration=duration)


    # Store Results
    query = "SELECT nextval('NetworkDimSequence')"
    networkId = _run_query(query, connection)[1,1]
    model.network_id = networkId
    model.network_construction_length = duration
    
    # Append NetworkDim data
    appender = DuckDB.Appender(connection, "NetworkDim")
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.town_id)
    DuckDB.append(appender, model.network_construction_length)
    DuckDB.append(appender, join(Serialize_Model(model), ","))
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append NetworkSCMLoad data
    if STORE_NETWORK_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "NetworkSCMLoad")

        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end

    return model.network_id
end

function _create_behaved_network!(network_id::Int, mask_distribution_type::String, vax_distribution_type::String, mask_portion::Int, vax_portion::Int, connection::DuckDB.DB)
    # Input validation
    @assert (mask_distribution_type == "Watts" || mask_distribution_type == "Random") "Unsupported mask distribution type $(mask_distribution_type) "
    @assert (vax_distribution_type == "Watts" || vax_distribution_type == "Random") "Unsupported vax distribution type $(vax_distribution_type)"
    @assert (0 <= mask_portion <= 100) "Mask Portion must be an integer from 0 to 100"
    @assert (0 <= vax_portion <= 100) "Vaccine Portion must be an integer from 0 to 100"

    model = _get_model_by_network_id(network_id, connection)
    model === nothing && return false

    # Apply Behavior to model
    model.mask_distribution_type = mask_distribution_type
    model.vax_distribution_type = vax_distribution_type
    model.mask_portion = mask_portion
    model.vax_portion = vax_portion
    Apply_Social_Behavior!(model)

    # Store results
    behaviorId = run_query("SELECT nextval('BehaviorDimSequence')", connection)[1,1]
    model.behavior_id = behaviorId
    
    # Append to BehaviorDim
    appender = DuckDB.Appender(connection, "BehaviorDim")
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.mask_distribution_type)
    DuckDB.append(appender, model.vax_distribution_type)
    DuckDB.append(appender, model.mask_portion)
    DuckDB.append(appender, model.vax_portion)
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append to AgentLoad
    appender = DuckDB.Appender(connection, "AgentLoad")
    for agentId in 1:model.init_pop_size
        DuckDB.append(appender, model.behavior_id)
        DuckDB.append(appender, agentId)
        DuckDB.append(appender, model[agentId].home)
        DuckDB.append(appender, Int(model[agentId].will_mask[1]))
        DuckDB.append(appender, Int(model[agentId].vaccinated))
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    return model.behavior_id
end

function _create_behaved_network!(model, mask_distribution_type::String, vax_distribution_type::String, mask_portion::Int, vax_portion::Int, connection::DuckDB.DB)
    # Input validation
    @assert (mask_distribution_type == "Watts" || mask_distribution_type == "Random") "Unsupported mask distribution type $(mask_distribution_type) "
    @assert (vax_distribution_type == "Watts" || vax_distribution_type == "Random") "Unsupported vax distribution type $(vax_distribution_type)"
    @assert (0 <= mask_portion <= 100) "Mask Portion must be an integer from 0 to 100"
    @assert (0 <= vax_portion <= 100) "Vaccine Portion must be an integer from 0 to 100"

    query = "SELECT COUNT(NetworkID) FROM NetworkDim WHERE NetworkID = $(model.network_id)"
    modelDetected = run_query(query, connection)[1,1]

    @assert (modelDetected != 0) "Failed to locate model in existing database" 

    # Apply Behavior to model
    model.mask_distribution_type = mask_distribution_type
    model.vax_distribution_type = vax_distribution_type
    model.mask_portion = mask_portion
    model.vax_portion = vax_portion
    Apply_Social_Behavior!(model)

    # Store results
    behaviorId = run_query("SELECT nextval('BehaviorDimSequence')", connection)[1,1]
    model.behavior_id = behaviorId
    
    # Append to BehaviorDim
    appender = DuckDB.Appender(connection, "BehaviorDim")
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.network_id)
    DuckDB.append(appender, model.mask_distribution_type)
    DuckDB.append(appender, model.vax_distribution_type)
    DuckDB.append(appender, model.mask_portion)
    DuckDB.append(appender, model.vax_portion)
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append to AgentLoad
    appender = DuckDB.Appender(connection, "AgentLoad")
    for agentId in 1:model.init_pop_size
        DuckDB.append(appender, model.behavior_id)
        DuckDB.append(appender, agentId)
        DuckDB.append(appender, model[agentId].home)
        DuckDB.append(appender, Int(model[agentId].will_mask[1]))
        DuckDB.append(appender, Int(model[agentId].vaccinated))
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    return model.behavior_id
end

function _create_epidemic!(behaviorId::Int, connection::DuckDB.DB; STORE_EPIDEMIC_SCM=false)
    model = _get_model_by_behavior_id(behaviorId, connection)
    model === nothing && return false

    Seed_Contagion!(model)
    Run_Model!(model)

    # Store Results
    epidemicId = run_query("SELECT nextval('EpidemicDimSequence')", connection)[1,1]
    model.epidemic_id = epidemicId

    # Append EpidemicDim data
    appender = DuckDB.Appender(connection, "EpidemicDim")
    DuckDB.append(appender, model.epidemic_id)
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.epidemic_statistics[1,1])
    DuckDB.append(appender, model.epidemic_statistics[1,2])
    DuckDB.append(appender, model.epidemic_statistics[1,3])
    DuckDB.append(appender, model.epidemic_statistics[1,4])
    DuckDB.append(appender, model.epidemic_statistics[1,5])
    DuckDB.append(appender, model.epidemic_statistics[1,6])
    DuckDB.append(appender, model.epidemic_statistics[1,7])
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append EpidemicLoad data
    appender = DuckDB.Appender(connection, "EpidemicLoad")
    for row in eachrow(model.epidemic_data)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Append TransmissionLoad data
    appender = DuckDB.Appender(connection, "TransmissionLoad")
    for row in eachrow(model.TransmissionNetwork)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Store Epidemic SCM
    if STORE_EPIDEMIC_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "EpidemicSCMLoad")
        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end

    return model.epidemic_id
end

function _create_epidemic!(model, connection::DuckDB.DB; STORE_EPIDEMIC_SCM=false)
    Seed_Contagion!(model)
    Run_Model!(model)

    # Store Results
    epidemicId = run_query("SELECT nextval('EpidemicDimSequence')", connection)[1,1]
    model.epidemic_id = epidemicId

    # Append EpidemicDim data
    appender = DuckDB.Appender(connection, "EpidemicDim")
    DuckDB.append(appender, model.epidemic_id)
    DuckDB.append(appender, model.behavior_id)
    DuckDB.append(appender, model.epidemic_statistics[1,1])
    DuckDB.append(appender, model.epidemic_statistics[1,2])
    DuckDB.append(appender, model.epidemic_statistics[1,3])
    DuckDB.append(appender, model.epidemic_statistics[1,4])
    DuckDB.append(appender, model.epidemic_statistics[1,5])
    DuckDB.append(appender, model.epidemic_statistics[1,6])
    DuckDB.append(appender, model.epidemic_statistics[1,7])
    DuckDB.end_row(appender)
    DuckDB.close(appender)

    # Append EpidemicLoad data
    appender = DuckDB.Appender(connection, "EpidemicLoad")
    for row in eachrow(model.epidemic_data)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.append(appender, row[4])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Append TransmissionLoad data
    appender = DuckDB.Appender(connection, "TransmissionLoad")
    for row in eachrow(model.TransmissionNetwork)
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, row[1])
        DuckDB.append(appender, row[2])
        DuckDB.append(appender, row[3])
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)

    # Store Epidemic SCM
    if STORE_EPIDEMIC_SCM
        socialContactVector = Get_Compact_Adjacency_Matrix(model)
        appender = DuckDB.Appender(connection, "EpidemicSCMLoad")
        population = model.init_pop_size
        epidemicSCMItr = 1
        for agent1 in 1:population
            for agent2 in agent1+1:population
                DuckDB.append(appender, model.epidemic_id)
                DuckDB.append(appender, agent1)
                DuckDB.append(appender, agent2)
                DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                DuckDB.end_row(appender)
                epidemicSCMItr += 1
            end
        end
        DuckDB.close(appender)
    end

    return model.epidemic_id
end

function _create_epidemic_distributed!(model, epidemic_runs::Int, connection::DuckDB.DB; STORE_EPIDEMIC_SCM=false)
    @assert epidemic_runs > -1 "epidemic_runs must be postive: $(epidemic_runs)"

    models = [deepcopy(model) for _ in 1:epidemic_runs]
    pmap(Seed_Contagion!, models; retry_delays = zeros(3))
    pmap(Run_Model!, models)

    epidemicIds = []
    for model in models
        # Store Results
        epidemicId = run_query("SELECT nextval('EpidemicDimSequence')", connection)[1,1]
        model.epidemic_id = epidemicId

        # Append EpidemicDim data
        appender = DuckDB.Appender(connection, "EpidemicDim")
        DuckDB.append(appender, model.epidemic_id)
        DuckDB.append(appender, model.behavior_id)
        DuckDB.append(appender, model.epidemic_statistics[1,1])
        DuckDB.append(appender, model.epidemic_statistics[1,2])
        DuckDB.append(appender, model.epidemic_statistics[1,3])
        DuckDB.append(appender, model.epidemic_statistics[1,4])
        DuckDB.append(appender, model.epidemic_statistics[1,5])
        DuckDB.append(appender, model.epidemic_statistics[1,6])
        DuckDB.append(appender, model.epidemic_statistics[1,7])
        DuckDB.end_row(appender)
        DuckDB.close(appender)

        # Append EpidemicLoad data
        appender = DuckDB.Appender(connection, "EpidemicLoad")
        for row in eachrow(model.epidemic_data)
            DuckDB.append(appender, model.epidemic_id)
            DuckDB.append(appender, row[1])
            DuckDB.append(appender, row[2])
            DuckDB.append(appender, row[3])
            DuckDB.append(appender, row[4])
            DuckDB.end_row(appender)
        end
        DuckDB.close(appender)

        # Append TransmissionLoad data
        appender = DuckDB.Appender(connection, "TransmissionLoad")
        for row in eachrow(model.TransmissionNetwork)
            DuckDB.append(appender, model.epidemic_id)
            DuckDB.append(appender, row[1])
            DuckDB.append(appender, row[2])
            DuckDB.append(appender, row[3])
            DuckDB.end_row(appender)
        end
        DuckDB.close(appender)

        # Store Epidemic SCM
        if STORE_EPIDEMIC_SCM
            socialContactVector = Get_Compact_Adjacency_Matrix(model)
            appender = DuckDB.Appender(connection, "EpidemicSCMLoad")
            population = model.init_pop_size
            epidemicSCMItr = 1
            for agent1 in 1:population
                for agent2 in agent1+1:population
                    DuckDB.append(appender, model.epidemic_id)
                    DuckDB.append(appender, agent1)
                    DuckDB.append(appender, agent2)
                    DuckDB.append(appender, socialContactVector[epidemicSCMItr])
                    DuckDB.end_row(appender)
                    epidemicSCMItr += 1
                end
            end
            DuckDB.close(appender)
        end
        push!(epidemicIds, model.epidemic_id)
    end

    return epidemicIds
end

# function _begin_simulations_faster(town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, town, STORE_NETWORK_SCM::Bool, STORE_EPIDEMIC_SCM::Bool, number_workers::Int)
#     # Ensure at least 2 threads are available, one main thread and one for the _dbWriterTask
#     @assert Threads.nthreads() > 1 "Not enough threads to run multi-threaded simulation. $(Threads.nthreads()) detected, at least 2 required"

#     # Build Workers
#     println("Activating $(Base.active_project()) everywhere")
#     addprocs(number_workers)
#     eval(macroexpand(RuralABMDriver,quote @everywhere using Pkg end))
#     @everywhere Pkg.instantiate()
#     eval(macroexpand(RuralABMDriver,quote @everywhere using RuralABMDriver end))
    
#     # Prepare town level channels
#     townLevelWrites = 1
#     networkLevelWrites = town_networks
#     behaviorLevelWrites = mask_levels * vaccine_levels * town_networks
#     epidemicLevelWrites = mask_levels * vaccine_levels * town_networks * runs
        
#     # Prepare pipeline layers
#     jobsChannel = RemoteChannel(()->Channel(2*number_workers))
#     writesChannel = RemoteChannel(()->Channel(2*number_workers))

#     populationModels = RemoteChannel(()->Channel(1)); 
#     stableModels = RemoteChannel(()->Channel(town_networks));
#     behavedModels = RemoteChannel(()->Channel(behaviorLevelWrites));

#     townIdChannel = RemoteChannel(()->Channel(townLevelWrites))
#     networkIdChannel = RemoteChannel(()->Channel(networkLevelWrites))
#     behaviorIdChannel = RemoteChannel(()->Channel(behaviorLevelWrites))
#     epidemicIdChannel = RemoteChannel(()->Channel(epidemicLevelWrites))

#     println("All channels created")

#     # On a separate thread begin the Writer() which handles all writes to the db
#     writerTask = Threads.@spawn _dbWriterTask(townLevelWrites, networkLevelWrites, behaviorLevelWrites, epidemicLevelWrites, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, writesChannel, populationModels, stableModels, behavedModels, townIdChannel, networkIdChannel, behaviorIdChannel, epidemicIdChannel)

#     # Run the raw models to establish a social network
#     for p in workers()
#         println("Spinning Up Worker $p")
#         remote_do(Spin_Up_Worker, p, jobsChannel, writesChannel, duration_days_network)
#     end

#     # Generate the initial model object
#     distribution_type[1] == 0 ? MaskDistributionType = "Random" : MaskDistributionType = "Watts"
#     distribution_type[2] == 0 ? VaxDistributionType = "Random" : VaxDistributionType = "Watts"

#     if town == "small" 
#         PopulationID = 1
#     elseif town == "large"
#         PopulationID = 2
#     end

#     println("Constructing Town")
#     model_raw , townDataSummaryDF, businessStructureDF, houseStructureDF = Construct_Town(joinpath("lib", "RuralABM", "data", "example_towns", "$(town)_town", "population.csv"), joinpath("lib", "RuralABM", "data", "example_towns", "$(town)_town", "businesses.csv"))
#     model_raw.population_id = PopulationID
#     model_raw.mask_distribution_type = MaskDistributionType
#     model_raw.vax_distribution_type = VaxDistributionType
#     model_raw.network_construction_length = duration_days_network
#     put!(writesChannel, (model_raw, "Town Level"))
    
#     # Store TownDim Level
#     println("Feeding Town Structure")
#     errormonitor(Threads.@spawn _populate_raw_model_channel(town_networks, jobsChannel, populationModels))    
#     errormonitor(Threads.@spawn _populate_unbehaved_models(mask_levels, vaccine_levels, town_networks, jobsChannel, stableModels))
#     errormonitor(Threads.@spawn _populate_seeded_models(runs, mask_levels, vaccine_levels, town_networks, jobsChannel, behavedModels))

#     # Let dbWriterTask finish
#     wait(writerTask)

#     # Clean-up
#     finalize(townIdChannel)
#     finalize(networkIdChannel)
#     finalize(behaviorIdChannel)
#     finalize(epidemicIdChannel)

#     # Kill wokrer processes
#     rmprocs(workers()...)
# end

# function _dbWriterTask(townLevelWrites, networkLevelWrites, behaviorLevelWrites, epidemicLevelWrites, STORE_NETWORK_SCM, STORE_EPIDEMIC_SCM, writesChannel, populationModels, stableModels, behavedModels, townIdChannel, networkIdChannel, behaviorIdChannel, epidemicIdChannel)
#     connection = _create_default_connection()

#     # Insert Id data
#     query = """SELECT nextval('TownDimSequence')"""
#     for _ in 1:townLevelWrites
#        put!(townIdChannel, _run_query(query, connection)[1,1]) 
#     end

#     query = """SELECT nextval('NetworkDimSequence')"""
#     for _ in 1:networkLevelWrites
#         put!(networkIdChannel, _run_query(query, connection)[1,1]) 
#     end

#     query = """SELECT nextval('BehaviorDimSequence')"""
#     for _ in 1:behaviorLevelWrites
#         put!(behaviorIdChannel, _run_query(query, connection)[1,1]) 
#     end

#     query = """SELECT nextval('EpidemicDimSequence')"""
#     for _ in 1:epidemicLevelWrites
#         put!(epidemicIdChannel, _run_query(query, connection)[1,1]) 
#     end
    
#     jobs = townLevelWrites + networkLevelWrites + behaviorLevelWrites + epidemicLevelWrites
#     @sync begin
#         for i in 1:jobs
#             model, task = take!(writesChannel)
    
#             if task == "Town Level"
#                 errormonitor(Threads.@spawn _append_town_structure(connection, model, populationModels, townIdChannel))
#             elseif task == "Network Level"
#                 errormonitor(Threads.@spawn _append_network_level_data(connection, model, STORE_NETWORK_SCM, stableModels, networkIdChannel))
#             elseif task == "Behavior Level"
#                 errormonitor(Threads.@spawn _append_behavior_level_data(connection, model, behavedModels, behaviorIdChannel))
#             elseif task == "Epidemic Level"
#                 errormonitor(Threads.@spawn _append_epidemic_level_data(connection, model, STORE_EPIDEMIC_SCM, epidemicIdChannel))
#             end
#             println("Jobs Complete: $i/$jobs")
#             if (i % 200 == 0) 
#                 println("Garbage Collecting")
#                 @everywhere GC.gc()
#             end
#         end
#     end 

#     DBInterface.close(connection)
# end

# function _populate_seeded_models(runs, mask_levels, vaccine_levels, networks, jobsChannel, behavedModels)
#     for _ in 1:(mask_levels * vaccine_levels * networks)
#         behavedModel = take!(behavedModels)
#         for _ in 1:runs
#             put!(jobsChannel, (behavedModel, "Run Epidemic"))
#         end
#     end
# end

# function _populate_unbehaved_models(mask_levels, vaccine_levels, networks, jobsChannel, stableModels)
#     # Compute target levels for masks and vaccines
#     mask_incr = floor(100/(mask_levels))
#     vacc_incr = floor(100/(vaccine_levels))

#     for _ in 1:networks
#         stableModel = take!(stableModels)
#         for mask_lvl in 0:(mask_levels-1)
#             for vacc_lvl in 0:(vaccine_levels-1)
#                 put!(jobsChannel, (stableModel, "Apply Behavior $(Int(mask_lvl*mask_incr)) $(Int(vacc_lvl*vacc_incr))"))
#             end
#         end
#     end

# end

# function _populate_raw_model_channel(networks, jobsChannel, populationModels)
#     model = take!(populationModels)

#     for _ in 1:networks
#         put!(jobsChannel, (model, "Build Network"))
#     end
# end

# function _append_epidemic_level_data(connection, model, STORE_EPIDEMIC_SCM, epidemicIdChannel)

#     model.epidemic_id = take!(epidemicIdChannel)

#     # Append EpidemicDim data
#     appender = DuckDB.Appender(connection, "EpidemicDim")
#     DuckDB.append(appender, model.epidemic_id)
#     DuckDB.append(appender, model.behavior_id)
#     DuckDB.append(appender, model.epidemic_statistics[1,1])
#     DuckDB.append(appender, model.epidemic_statistics[1,2])
#     DuckDB.append(appender, model.epidemic_statistics[1,3])
#     DuckDB.append(appender, model.epidemic_statistics[1,4])
#     DuckDB.append(appender, model.epidemic_statistics[1,5])
#     DuckDB.append(appender, model.epidemic_statistics[1,6])
#     DuckDB.append(appender, model.epidemic_statistics[1,7])
#     DuckDB.end_row(appender)
#     DuckDB.close(appender)

#     # Append EpidemicLoad data
#     appender = DuckDB.Appender(connection, "EpidemicLoad")
#     for row in eachrow(model.epidemic_data_daily)
#         DuckDB.append(appender, model.epidemic_id)
#         DuckDB.append(appender, row[1])
#         DuckDB.append(appender, row[2])
#         DuckDB.append(appender, row[3])
#         DuckDB.append(appender, row[4])
#         DuckDB.end_row(appender)
#     end
#     DuckDB.close(appender)

#     # Append TransmissionLoad data
#     appender = DuckDB.Appender(connection, "TransmissionLoad")
#     for row in eachrow(model.TransmissionNetwork)
#         DuckDB.append(appender, model.epidemic_id)
#         DuckDB.append(appender, row[1])
#         DuckDB.append(appender, row[2])
#         DuckDB.append(appender, row[3])
#         DuckDB.end_row(appender)
#     end
#     DuckDB.close(appender)

#     # Store Epidemic SCM
#     if STORE_EPIDEMIC_SCM
#         socialContactVector = Get_Compact_Adjacency_Matrix(model)
#         appender = DuckDB.Appender(connection, "EpidemicSCMLoad")
#         population = model.init_pop_size
#         epidemicSCMItr = 1
#         for agent1 in 1:population
#             for agent2 in agent1+1:population
#                 DuckDB.append(appender, model.epidemic_id)
#                 DuckDB.append(appender, agent1)
#                 DuckDB.append(appender, agent2)
#                 DuckDB.append(appender, socialContactVector[epidemicSCMItr])
#                 DuckDB.end_row(appender)
#                 epidemicSCMItr += 1
#             end
#         end
#         DuckDB.close(appender)
#     end
# end

# function _append_behavior_level_data(connection, model, behavedModels, behaviorIdChannel)

#     model.behavior_id = take!(behaviorIdChannel)
#     put!(behavedModels, model)
    
#     # Append to BehaviorDim
#     appender = DuckDB.Appender(connection, "BehaviorDim")
#     DuckDB.append(appender, model.behavior_id)
#     DuckDB.append(appender, model.network_id)
#     DuckDB.append(appender, model.mask_portion)
#     DuckDB.append(appender, model.vax_portion)
#     DuckDB.end_row(appender)
#     DuckDB.close(appender)

#     # Append to AgentLoad
#     appender = DuckDB.Appender(connection, "AgentLoad")
#     for agentId in 1:model.init_pop_size
#         DuckDB.append(appender, model.behavior_id)
#         DuckDB.append(appender, agentId)
#         DuckDB.append(appender, model[agentId].home)
#         DuckDB.append(appender, Int(model[agentId].will_mask[1]))
#         DuckDB.append(appender, Int(model[agentId].vaccinated))
#         DuckDB.end_row(appender)
#     end
#     DuckDB.close(appender)
# end

# function _append_town_structure(connection, model, populationModels, townIdChannel)
#     model.town_id = take!(townIdChannel)
#     put!(populationModels, model)

#     # Populate TownDim
#     appender = DuckDB.Appender(connection, "TownDim")
#     DuckDB.append(appender, model.town_id)
#     DuckDB.append(appender, model.population_id)
#     DuckDB.append(appender, length(model.business))
#     DuckDB.append(appender, length(model.houses))
#     DuckDB.append(appender, length(model.school))
#     DuckDB.append(appender, length(model.daycare))
#     DuckDB.append(appender, length(model.community_gathering))
#     DuckDB.append(appender, length(model.number_adults))
#     DuckDB.append(appender, length(model.number_elders))
#     DuckDB.append(appender, length(model.number_children))
#     DuckDB.append(appender, length(model.number_empty_businesses))
#     DuckDB.append(appender, model.mask_distribution_type)
#     DuckDB.append(appender, model.vax_distribution_type)
#     DuckDB.end_row(appender)
#     DuckDB.close(appender)

#     # Populate BusinessLoad
#     appender = DuckDB.Appender(connection, "BusinessLoad")
#     for row in eachrow(model.business_structure_dataframe)
#         DuckDB.append(appender, model.town_id)
#         DuckDB.append(appender, row[1])
#         DuckDB.append(appender, row[2])
#         DuckDB.append(appender, row[3])
#         DuckDB.end_row(appender)
#     end
#     DuckDB.close(appender)

#     # Populate HouseholdLoad
#     appender = DuckDB.Appender(connection, "HouseholdLoad")
#     for row in eachrow(model.household_structure_dataframe)
#         DuckDB.append(appender, model.town_id)
#         DuckDB.append(appender, row[1])
#         DuckDB.append(appender, row[2])
#         DuckDB.append(appender, row[3])
#         DuckDB.append(appender, row[4])
#         DuckDB.end_row(appender)
#     end
#     DuckDB.close(appender)
# end

# function _append_network_level_data(connection, model, STORE_NETWORK_SCM, stableModels, networkIdChannel)
#     model.network_id = take!(networkIdChannel)
#     put!(stableModels, model)    
    
#     # Append NetworkDim data
#     appender = DuckDB.Appender(connection, "NetworkDim")
#     DuckDB.append(appender, model.network_id)
#     DuckDB.append(appender, model.town_id)
#     DuckDB.append(appender, model.network_construction_length)
#     DuckDB.end_row(appender)
#     DuckDB.close(appender)

#     # Append NetworkSCMLoad data
#     if STORE_NETWORK_SCM
#         socialContactVector = Get_Compact_Adjacency_Matrix(model)
#         appender = DuckDB.Appender(connection, "NetworkSCMLoad")

#         population = model.init_pop_size
#         epidemicSCMItr = 1
#         for agent1 in 1:population
#             for agent2 in agent1+1:population
#                 DuckDB.append(appender, model.epidemic_id)
#                 DuckDB.append(appender, agent1)
#                 DuckDB.append(appender, agent2)
#                 DuckDB.append(appender, socialContactVector[epidemicSCMItr])
#                 DuckDB.end_row(appender)
#                 epidemicSCMItr += 1
#             end
#         end
#         DuckDB.close(appender)
#     end
# end

# function _store_epidemic_scm(SocialContactMatrices1DF, epidemicID)
#     connection = _create_default_connection()

#     SocialContactMatrices1DF = DataFrame(SocialContactMatrices1, :auto)
#     for SocialContactMatrix in eachcol(SocialContactMatrices1DF)
#         EpidemicSCMAppender = DuckDB.Appender(connection, "EpidemicSCMLoad")
        
#         Population = SocialContactMatrix[1] 
#         EpidemicSCMItr = 2
#         for agent1 in 1:Population
#             for agent2 in agent1+1:Population
#                 DuckDB.append(EpidemicSCMAppender, epidemicID)
#                 DuckDB.append(EpidemicSCMAppender, agent1)
#                 DuckDB.append(EpidemicSCMAppender, agent2)
#                 DuckDB.append(EpidemicSCMAppender, SocialContactMatrix[EpidemicSCMItr])
#                 DuckDB.end_row(EpidemicSCMAppender)
#                 EpidemicSCMItr += 1
#             end
#         end
#         DuckDB.close(EpidemicSCMAppender)
#     end
# end

# function _store_agent_load(model, behaviorID)
#     connection = _create_default_connection()
#     AgentLoadAppender = DuckDB.Appender(connection, "AgentLoad")
#     for agentID in 1:model.init_pop_size
#         DuckDB.append(AgentLoadAppender, behaviorID)
#         DuckDB.append(AgentLoadAppender, agentID)
#         DuckDB.append(AgentLoadAppender, model[agentID].home)
#         DuckDB.append(AgentLoadAppender, Int(model[agentID].will_mask[1]))
#         DuckDB.append(AgentLoadAppender, Int(model[agentID].vaccinated))
#         DuckDB.end_row(AgentLoadAppender)
#     end
#     DuckDB.close(AgentLoadAppender)
#     disconnect_from_database!(connection)

#     return true
# end

# function _store_epidemic_dim_entry(summaryStatistics, behaviorID)
#     connection = _create_default_connection()

#     query = """SELECT nextval('EpidemicDimSequence')"""
#     result = _run_query(query, connection) 
#     epidemicID = result[1,1]

#     appender = DuckDB.Appender(connection, "EpidemicDim")
#     DuckDB.append(appender, epidemicID)
#     DuckDB.append(appender, behaviorID)
#     DuckDB.append(appender, summaryStatistics[1,1])
#     DuckDB.append(appender, summaryStatistics[1,2])
#     DuckDB.append(appender, summaryStatistics[1,3])
#     DuckDB.append(appender, summaryStatistics[1,4])
#     DuckDB.append(appender, summaryStatistics[1,5])
#     DuckDB.append(appender, summaryStatistics[1,6])
#     DuckDB.append(appender, summaryStatistics[1,7])

#     DuckDB.end_row(appender)
#     DuckDB.close(appender)

#     disconnect_from_database!(connection)

#     return epidemicID
# end

# function _store_behavior_dim_entry(NetworkID, MaskVaxID)
#     connection = _create_default_connection()
#     query = """
#         SELECT nextval('BehaviorDimSequence')
#     """
#     Result = _run_query(query, connection) 
#     BehaviorID = Result[1,1]

#     Appender = DuckDB.Appender(connection, "BehaviorDim")
#     DuckDB.append(Appender, BehaviorID)
#     DuckDB.append(Appender, NetworkID)
#     DuckDB.append(Appender, MaskVaxID)
#     DuckDB.end_row(Appender)
#     DuckDB.close(Appender)

#     disconnect_from_database!(connection)

#     return BehaviorID
# end