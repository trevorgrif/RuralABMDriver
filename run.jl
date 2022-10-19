using Distributed
@everywhere using Pkg
@everywhere Pkg.activate(".")
@everywhere using RuralABMDriver

Run_RuralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   MODEL_RUNS = 100,
   TOWN_NAMES = ["small"],
   OUTPUT_TOWN_INDEX = 1,
   OUTPUT_DIR = "output"
   )
