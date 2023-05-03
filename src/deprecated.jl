function run_ruralABM_deprecated(;
    SOCIAL_NETWORKS = 10,
    NETWORK_LENGTH = 30,
    MASKING_LEVELS = 5,
    VACCINATION_LEVELS = 5,
    DISTRIBUTION_TYPE = [0, 0],
    MODEL_RUNS = 100,
    TOWN_NAMES = ["small"],
    OUTPUT_TOWN_INDEX = 1,
    OUTPUT_DIR = "output"
    )
    # Store Model in End State
    build_output_dirs_deprecated(SOCIAL_NETWORKS, TOWN_NAMES, OUTPUT_TOWN_INDEX, OUTPUT_DIR)

    # Store Model in End State
    begin_simulations_deprecated(SOCIAL_NETWORKS, MASKING_LEVELS, VACCINATION_LEVELS, DISTRIBUTION_TYPE, MODEL_RUNS, NETWORK_LENGTH, TOWN_NAMES, OUTPUT_TOWN_INDEX, OUTPUT_DIR)
end

"""
    build_output_dirs(x, l, y, dir)

Generate the directories for `x` social networks for towns in the list `l` at the root directory `dir` and append `y` to the town names.
"""
function build_output_dirs_deprecated(num_networks::Int, towns, output_town_index::Int, OUTPUT_DIR)
    # Non-dynamic directories
    !isdir("$(OUTPUT_DIR)") && mkdir("$(OUTPUT_DIR)")

    # Dynamic directories
    for town in towns
        !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)")
        !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/SCM") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/SCM")
        !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN")
        !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED")
        for i in 1:num_networks
            !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN/$(@sprintf("%.3d",i))") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN/$(@sprintf("%.3d",i))")
            !isdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED/$(@sprintf("%.3d",i))") && mkdir("$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED/$(@sprintf("%.3d",i))")
        end
    end
end

"""
    begin_simulations(town_networks:, mask_levels, vaccine_levels, runs, duration_days_network, towns, output_town_index, OUTPUT_DIR)

Run RuralABM simulations based on the values passed. See documentation of Run_RuralABM for details.
"""
function begin_simulations_deprecated(town_networks::Int, mask_levels::Int, vaccine_levels::Int, distribution_type::Vector{Int64}, runs::Int, duration_days_network, towns, output_town_index::Int, OUTPUT_DIR)
    # Compute target levels for masks and vaccines
    mask_incr = floor(100/mask_levels)
    vacc_incr = floor(100/vaccine_levels)

    for town in towns
        # Build Town Model
        model_raw , townDataSummaryDF, businessStructureDF, houseStructureDF = Construct_Town("lib/RuralABM/data/example_towns/$(town)_town/population.csv", "lib/RuralABM/data/example_towns/$(town)_town/businesses.csv")

        # Store Town Structure Data
        CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/town_summary.csv", townDataSummaryDF)
        CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/town_businesses.csv", businessStructureDF)
        CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/town_households.csv", houseStructureDF)

        # Generate social network models and collect compact adjacency matrices
        ResultsPostSocialNetworks = pmap((x,y) -> Run_Model!(x, duration = y), [deepcopy(model_raw) for x in 1:town_networks], fill(duration_days_network, town_networks))
        ModelSocialNetworks = [x[1] for x in ResultsPostSocialNetworks]
        SocialContactMatrices0 = [x[4] for x in ResultsPostSocialNetworks]

        # Label matrices by index and convert to dataframe
        SocialContactMatrices0 = pmap(insert!, SocialContactMatrices0, fill(1, town_networks), [i for i in 1:town_networks])
        SocialContactMatrices0DF = DataFrame(SocialContactMatrices0, :auto)

        # Store social network contact matrices
        CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/SCM/precontagion.csv", SocialContactMatrices0DF, header=false)

        # Forced Garbage Collection
        ResultsPostSocialNetworks = 0
        SocialContactMatrices0 = 0
        SocialContactMatrices0DF = 0
        GC.gc()

        SocialNetworkIndex = 1
        for ModelSocialNetwork in ModelSocialNetworks
            # Initialize dataframe for storing all epidemic data
            EpidemicDF = DataFrame()

            for mask_lvl in 0:mask_incr:99
                for vacc_lvl in 0:vacc_incr:99
                    # Make a copy of the model
                    model_precontagion = deepcopy(ModelSocialNetwork)

                    # Apply masking to town
                    if distribution_type[1] == 0
                        mask_id_arr = Get_Portion_Random(model_precontagion, mask_lvl/100, [(x)->x.age >= 2])
                    elseif distribution_type[1] == 1
                        mask_id_arr = Get_Portion_Watts(model_precontagion, mask_lvl/100)
                    end
                    Update_Agents_Attribute!(model_precontagion, mask_id_arr, :will_mask, [true, true, true])

                    # Apply vaccination level to town
                    if distribution_type[2] == 0
                        vaccinated_id_arr = Get_Portion_Random(model_precontagion, vacc_lvl/100, [(x)-> x.age > 4 && x.age < 18, (x)->x.age >= 18], [0.34, 0.66])
                    elseif distribution_type[2] == 1
                        vaccinated_id_arr = Get_Portion_Watts(model_precontagion, vacc_lvl/100)
                    end
                    Update_Agents_Attribute!(model_precontagion, vaccinated_id_arr, :status, :V)
                    Update_Agents_Attribute!(model_precontagion, vaccinated_id_arr, :vaccinated, true)

                    # Collect and store each agents home and social behaviors
                    AgentsSocialBehaviorDF = DataFrame(AgentID = Int64[], AgentHouseholdID = Int64[], IsMasking = Bool[], IsVaxed = Bool[])
                    for idx in 1:model_precontagion.init_pop_size
                        append!(AgentsSocialBehaviorDF, DataFrame(AgentID = idx, AgentHouseholdID = model_precontagion[idx].home, IsMasking = (idx in mask_id_arr), IsVaxed = (idx in vaccinated_id_arr)))
                    end

                    # Build arrays for pmap
                    ModelContagionArr = [deepcopy(model_precontagion) for x in 1:runs]
                    EDOutputLocations = []
                    TNOutputLocations = []

                    for j in 1:runs
                        push!(EDOutputLocations, "$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED/$(@sprintf("%.3d",SocialNetworkIndex))/$(@sprintf("%.2d", Int(mask_lvl)))_$(@sprintf("%.2d", Int(vacc_lvl)))_$(@sprintf("%.3d",j)).csv")
                        push!(TNOutputLocations, "$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN/$(@sprintf("%.3d",SocialNetworkIndex))/$(@sprintf("%.2d", Int(mask_lvl)))_$(@sprintf("%.2d", Int(vacc_lvl)))_$(@sprintf("%.3d",j)).csv")
                    end

                    # Seed and run model in parallel
                    ModelContagionArr = pmap(Seed_Contagion!, ModelContagionArr)
                    ModelRunsOutput = pmap(Run_Model!, ModelContagionArr)

                    # Gather output
                    SocialContactMatrices1 = [x[4] for x in ModelRunsOutput]
                    SummaryStatistics = [x[5] for x in ModelRunsOutput]

                    # Analyze the output
                    AgentDataArrayDaily = pmap(Get_Daily_Agentdata, [x[2] for x in ModelRunsOutput])

                    SocialContactMatrices1 = pmap(insert!, SocialContactMatrices1, fill(1, runs), [x for x in 1:runs])
                    SocialContactMatrices1DF = DataFrame(SocialContactMatrices1, :auto)

                    SummaryStatistics = pmap(insertcols!, SummaryStatistics, fill(1, runs), [:Idx => x for x in 1:runs])
                    SummaryStatistics = pmap(insertcols!, SummaryStatistics, fill(1, runs), [:VaxLvl => vacc_lvl for x in 1:runs])
                    SummaryStatistics = pmap(insertcols!, SummaryStatistics, fill(1, runs), [:MaskLvl => mask_lvl for x in 1:runs])
                    SummaryStatistics = pmap(insertcols!, SummaryStatistics, fill(1, runs), [:NetworkIdx => SocialNetworkIndex for x in 1:runs])
                    for df in SummaryStatistics
                        append!(EpidemicDF, df)
                    end

                    # Store the output (SCM IS FAILING ON RAM< NEED MULTIPLE FILES INSTEAD OF ONE)
                    pmap(CSV.write, EDOutputLocations, AgentDataArrayDaily)
                    pmap(CSV.write, TNOutputLocations, [x[3] for x in ModelRunsOutput])
                    CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/TN/$(@sprintf("%.3d",SocialNetworkIndex))/$(@sprintf("%.2d", Int(mask_lvl)))_$(@sprintf("%.2d", Int(vacc_lvl)))_Agent_Behavior.csv", AgentsSocialBehaviorDF)
                    CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/ED/summary_$(@sprintf("%.3d",SocialNetworkIndex)).csv", EpidemicDF)
                    CSV.write("$(OUTPUT_DIR)/$(town)_$(output_town_index)/SCM/postcontagion_$(@sprintf("%.2d", Int(mask_lvl)))_$(@sprintf("%.2d", Int(vacc_lvl)))_$(@sprintf("%.3d",SocialNetworkIndex)).csv", SocialContactMatrices1DF, header=false)

                    # Forced Garbage Collection
                    model_precontagion = 0
                    mask_id_arr = 0
                    vaccinated_id_arr = 0
                    AgentsSocialBehaviorDF = 0
                    ModelContagionArr = 0
                    EDOutputLocations = 0
                    TNOutputLocations = 0
                    ModelRunsOutput = 0
                    SocialContactMatrices1 = 0
                    SocialContactMatrices1DF = 0
                    SummaryStatistics = 0
                    AgentDataArrayDaily = 0
                    GC.gc()
                end
            end
            SocialNetworkIndex += 1

            # Forced Garbage Collection
            EpidemicDF = 0
            GC.gc()
        end
    end
end