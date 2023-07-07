""" 
    Run_RuralABM()

Run the RuralABM package with default parameters.

# Arguments
- `SOCIAL_NETWORKS=10`: Multiplicity of creating town social networks (Range: 1 -> infty).
- `NETWORK_LENGTH=30`: Length in days the model will be run to generate a social network (Range: 1 -> infty).
- `MASKING_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "2" generates [0,50] (Range: 0 -> 100).
- `VACCINATION_LEVELS=5`: Evenly split going from 0 to 100 (exclusive) i.e "4" generates [0,25,50,75] (Range: 0 -> 100).
- `MODEL_RUNS=100`: Multiplicity model runs with disease spread (Range: 1 -> infty).
- `TOWN_NAMES=["Dubois"]`: Towns which will be run. Ensure input data exist for target towns.
- `OUTPUT_TOWN_INDEX=1`: Index value appended to the town name in the output file directory.
- `OUTPUT_DIR="../output": Default output directory location.
"""
function run_ruralABM(;
    SOCIAL_NETWORKS = 10,
    NETWORK_LENGTH = 30,
    MASKING_LEVELS = 5,
    VACCINATION_LEVELS = 5,
    DISTRIBUTION_TYPE = [0, 0],
    MODEL_RUNS = 100,
    TOWN_NAMES = ["small"],
    )

    _run_ruralABM(
        SOCIAL_NETWORKS = SOCIAL_NETWORKS,
        NETWORK_LENGTH = NETWORK_LENGTH,
        MASKING_LEVELS = MASKING_LEVELS,
        VACCINATION_LEVELS = VACCINATION_LEVELS,
        DISTRIBUTION_TYPE = DISTRIBUTION_TYPE,
        MODEL_RUNS = MODEL_RUNS,
        TOWN_NAMES = TOWN_NAMES,
    )
end

"""
    Run_Query(query; connection = create_default_connection())

Run a query on the database.
"""
function run_query(query; connection = _create_default_connection())
    _run_query(query, connection = connection)
end

# Make a global variable to store connection details, this will be called 
function create_database_structure()    
    _create_database_structure()
end

function drop_database_structure()
    _drop_database_structure()
end

function analyze_landing(connection = _create_default_connection())
    _load_staging_tables(connection)
end

function analyze_staging(connection = _create_default_connection())
    _load_fact_tables(connection)
end
    
