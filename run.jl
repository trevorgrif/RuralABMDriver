

con = connect_to_database()
run_simulations(                                                                                                                                                                                                                                                         
   1,                                                                                                                                                                                                                                             
   RuralABMDriver.network_parameters(30),                                                                                                                                                                                                                                               
   10,                                                                                                                                                                                                                                                                                   
   RuralABMDriver.behavior_parameters("Watts", "Random", 5, 5),                                                                                                                                                                                                                          
   1,                                                                                                                                                                                                                                                                                   
   100,                                                                                                                                                                                                                                                                                  
   con,                                                                                                                                                                                                                                                                                 
   STORE_NETWORK_SCM=true,                                                                                                                                                                                                                                                              
   STORE_EPIDEMIC_SCM=true                                                                                                                                                                                                                                                              
   )
disconnect_from_database!(con)
vacuum_database()

con = connect_to_database()
run_simulations(                                                                                                                                                                                                                                                         
   1,                                                                                                                                                                                                                                             
   RuralABMDriver.network_parameters(30),                                                                                                                                                                                                                                               
   10,                                                                                                                                                                                                                                                                                   
   RuralABMDriver.behavior_parameters("Watts", "Random", 5, 5),                                                                                                                                                                                                                          
   1,                                                                                                                                                                                                                                                                                   
   100,                                                                                                                                                                                                                                                                                  
   con,                                                                                                                                                                                                                                                                                 
   STORE_NETWORK_SCM=true,                                                                                                                                                                                                                                                              
   STORE_EPIDEMIC_SCM=true                                                                                                                                                                                                                                                              
   )
disconnect_from_database!(con)
vacuum_database()

con = connect_to_database()
run_simulations(                                                                                                                                                                                                                                                         
   1,                                                                                                                                                                                                                                             
   RuralABMDriver.network_parameters(30),                                                                                                                                                                                                                                               
   10,                                                                                                                                                                                                                                                                                   
   RuralABMDriver.behavior_parameters("Random", "Watts", 5, 5),                                                                                                                                                                                                                          
   1,                                                                                                                                                                                                                                                                                   
   100,                                                                                                                                                                                                                                                                                  
   con,                                                                                                                                                                                                                                                                                 
   STORE_NETWORK_SCM=true,                                                                                                                                                                                                                                                              
   STORE_EPIDEMIC_SCM=true                                                                                                                                                                                                                                                              
   )
disconnect_from_database!(con)
vacuum_database()

con = connect_to_database()
run_simulations(                                                                                                                                                                                                                                                         
   1,                                                                                                                                                                                                                                             
   RuralABMDriver.network_parameters(30),                                                                                                                                                                                                                                               
   10,                                                                                                                                                                                                                                                                                   
   RuralABMDriver.behavior_parameters("Watts", "Watts", 5, 5),                                                                                                                                                                                                                          
   1,                                                                                                                                                                                                                                                                                   
   100,                                                                                                                                                                                                                                                                                  
   con,                                                                                                                                                                                                                                                                                 
   STORE_NETWORK_SCM=true,                                                                                                                                                                                                                                                              
   STORE_EPIDEMIC_SCM=true                                                                                                                                                                                                                                                              
   )
disconnect_from_database!(con)
vacuum_database()

