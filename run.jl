using Distributed
@everywhere using Pkg
@everywhere Pkg.activate(".")
@everywhere Pkg.instantiate()
@everywhere using RuralABMDriver

RuralABMDriver.run_ruralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   DISTRIBUTION_TYPE = [0, 0], #Order is [MASK, VAX], 0 = Random, 1 = Watts
   MODEL_RUNS = 100,
   TOWN_NAMES = ["small"],
   )
