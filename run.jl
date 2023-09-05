RuralABMDriver.run_ruralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   DISTRIBUTION_TYPE = [0, 0], #Order is [MASK, VAX], 0 = Random, 1 = Watts
   MODEL_RUNS = 100,
   TOWN_NAMES = "small",
   STORE_NETWORK_SCM = true,
   STORE_EPIDEMIC_SCM = true,
   NUMBER_WORKERS = 32
   )

RuralABMDriver.run_ruralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   DISTRIBUTION_TYPE = [0, 1], #Order is [MASK, VAX], 0 = Random, 1 = Watts
   MODEL_RUNS = 100,
   TOWN_NAMES = "small",
   STORE_NETWORK_SCM = true,
   STORE_EPIDEMIC_SCM = true,
   NUMBER_WORKERS = 32
   )

RuralABMDriver.run_ruralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   DISTRIBUTION_TYPE = [1, 0], #Order is [MASK, VAX], 0 = Random, 1 = Watts
   MODEL_RUNS = 100,
   TOWN_NAMES = "small",
   STORE_NETWORK_SCM = true,
   STORE_EPIDEMIC_SCM = true,
   NUMBER_WORKERS = 32
   )

RuralABMDriver.run_ruralABM(
   SOCIAL_NETWORKS = 10,
   NETWORK_LENGTH = 30,
   MASKING_LEVELS = 5,
   VACCINATION_LEVELS = 5,
   DISTRIBUTION_TYPE = [1, 1], #Order is [MASK, VAX], 0 = Random, 1 = Watts
   MODEL_RUNS = 100,
   TOWN_NAMES = "small",
   STORE_NETWORK_SCM = true,
   STORE_EPIDEMIC_SCM = true,
   NUMBER_WORKERS = 32
   )