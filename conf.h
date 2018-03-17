#define BENCH			counter_bench
//#define BENCH			dis_bench
//#define BENCH			rnw1_bench
//#define BENCH			rwn_bench
//#define BENCH			mcas_bench

#define ENABLE_THREADS
#define ERR_CHK

//#define MAIN_BLACKSCHOLES		main
#define MAIN_SWAPTIONS		main
//#define MAIN_FLUIDANIMATE 	main
//#define MAIN_FLUIDANIMATE_SERIAL	main
//#define MAIN_FULIDCMP			main
//#define MAIN_MICROBENCH		main
//#define MAIN_KMEANS			main
//#define MAIN_GENOME			main
//#define MAIN_VACATION			main
//#define MAIN_SSCA2			main
//#define MAIN_LABYRINTH			main
//#define MAIN_INTRUDER			main
//#define MAIN_ATOMIC			main
//#define MAIN_HASH				main
//#define MAIN_OWB_CASES		main
//#define MAIN_SCHED			main

//#define O_API	o_api_dummy
//#define TRANSACTION dummy::Transaction
//#define O_API	o_api_lock
//#define O_API	o_api_tl2
//#define O_API	o_api_norec
//#define O_API	o_api_undolog_invis

//#define O_API	o_api_otl2
//#define O_API	o_api_onorec
//#define O_API	o_api_undolog_invis_ordered
//#define O_API	o_api_stmlite
//#define TRANSACTION commons::AbstractTransaction



//#define O_API	o_api_undolog_vis
//#define O_API	o_api_undolog_vis_ordered
//#define TRANSACTION undolog_vis::Transaction



//#define O_API	o_api_owb
//#define O_API	o_api_owb_plus
//#define O_API	o_api_owb_plus_plus
//#define TRANSACTION owb::Transaction



//#define O_API	o_api_oul_speculative
//#define O_API	o_api_oul_speculative_plus
//#define O_API	o_api_oul_speculative_plus_plus
//#define TRANSACTION oul_speculative::Transaction



#define O_API	o_api_oul_steal
//#define O_API	o_api_oul_steal_plus
//#define O_API	o_api_oul_steal_plus_plus
#define TRANSACTION oul_steal::Transaction

#define STM_BITS_32
//#define STM_BITS_64

#define TM_INLINE   __attribute__((always_inline))
