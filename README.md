# RuralABMDriver

# Running the Model
ABM_Bulk_Run provides the framework for running and extracting data on the ABM for multiple target towns. To begin a bulk run on the ABM simply modify and run `run.jl` in Julia with  

    include("run.jl")

or by passing the script to Julia at the command line with

    julia run.jl

# Altering Script Parameters
Bulk run variables can be altered within `run.jl` and are described below:

- **SOCIAL_NETWORKS**: Multiplicity of social network construction for each town construction (i.e. number of times run_model() is called without contagion)  
- **NETWORK_LENGTH**: Length in days each model will be run without contagion  
- **MASKING_LEVELS**: Number of evenly split partitions for masking levels (e.g. "4" generates masking proportions of [0,25,50,75])  
- **VACCINATION_LEVELS**: Number of evenly split partitions for vaccination levels (e.g. "5" generates masking proportions of [0,20,40,60,80])  
- **MODEL_RUNS**: Multiplicity of model runs with contagion   
- **TOWN_NAMES**: Array of strings containing the target town names (Available towns: "Dubois" and "Grangeville")  
- **OUTPUT_TOWN_INDEX**: Int appended to outfile file name to index town constructions

# Structure of the Output Folder
The script `run.jl` will generate an "output" directory which contains certain epidemiology data. Due to the complexity and multiplicity of the model, the output file gets quite large and has a directory hierarchy system to assist in sorting the results. In order to understand the output structure it'll be helpful to remember the four critical stochastic steps in the model for each town:

1. Town Construction
2. Run model without contagion to build a social network
3. Distribute masking and vaccination behaviors to the agents
4. Run the model with contagion

Town constructions are indexed by an underscore ("_") following the name of the town and a number (e.g. Seattle_1, Austin_3,...).

Pre-contagion social networks each have their own branch in the directory. These branches are placed one step below the four main output categories:

1. AD (Agent Data)
2. SCM_0 (Social Contact Matrix Pre-Contagion)
3. SCM_1 (Social Contact Matrix Post-Contagion)
4. TN (Transmission Network)

For example, after step *(2)* the i-th social network of the 2nd town construction of Whoville is stored out as an adjacency matrix here:

    output/Whoville_2/SCM_0/i.jld2

For all data tracked during or after step *(4)* the file location reveals which model was used to generate the data. For example, after step *(4)* the final social network is stored out and can be found in

    output/town_name/SCM_1/i/j

where *j* is the index of the *i*-th town construction. Data in these folders will follow the naming convention

    masklvl_vacclvl_k.jld2

where `masklvl` and `vacclvl` are integers corresponding to the propotion of the population masking and vaccinated respectively. The integer *k* represents the index of the run using the base model. For example, `output/town_name/SCM_1/i/j/20_50_003.csv` would be the contact matrix of the 3rd simulation ran on the *j*-th social network of the *i*-th town construction with 20 percent of the town masking and 50 percent of the town vaccinated.

Transmission networks and agent data are also stored out after step *(4)* and follow the same conventions of *SCM_1* but are stored respectively in:

    output/TN
    output/AD

The agent data includes the daily count of infected, susceptible, recovered, and population.

# Description of Collected Data
- **Social Contact Matrices**: adjaceny matrices where each component is divided by the total time steps in hours.
- **Transmission Network**: Three column csv with *agent_id*, *infected_by*, and *time_of_infection*
- (NOT YET EXTRACTED) At the end of each day populate a row in a DataFrame with certain agent data:
   + ID
   + Covid attitudes (i.e. masking and vaccination behavior)
   + Disease status (:R, :S, :I, :D)
   + SIC code for workplace
   + SIC code for community gathering


[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://trevorgrif.github.io/RuralABMManager.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://trevorgrif.github.io/RuralABMManager.jl/dev/)
[![Build Status](https://travis-ci.com/trevorgrif/RuralABMManager.jl.svg?branch=master)](https://travis-ci.com/trevorgrif/RuralABMManager.jl)
