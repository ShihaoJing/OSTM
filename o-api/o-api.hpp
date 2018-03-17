#ifndef O_API_HPP_
#define O_API_HPP_

#include <setjmp.h>

#include "../algs/commons.hpp"

#include "../algs/dummy.hpp"
#include "../algs/owb.hpp"
#include "../algs/tl2.hpp"
#include "../algs/norec.hpp"
#include "../algs/undolog_invis.hpp"
#include "../algs/undolog_vis.hpp"
#include "../algs/oul_speculative.hpp"
#include "../algs/oul_steal.hpp"
#include "../algs/stmlite.hpp"
#include "../algs/stmlite-unordered.hpp"

#define GET_TX			commons::AbstractTransaction* getTx();
//#define GET_TX			dummy::Transaction* getTx();

#define API																\
		int get_workers_count();										\
		void init(int workers);											\
		void init_thread();												\
		void run_in_order(void (func) (void**), void** args, int age);	\
		void wait_till_finish();										\
		void print_statistics();										\

namespace o_api_lock{
	GET_TX
	API
	void cs_start();
	void cs_end();
};

namespace o_api_dummy 		{ GET_TX API };

namespace o_api_tl2	 		{ GET_TX API };
namespace o_api_otl2 		{ GET_TX API };

namespace o_api_norec	 	{ GET_TX API };
namespace o_api_onorec 		{ GET_TX API };

namespace o_api_owb 			{ owb::Transaction* getTx(); API };
namespace o_api_owb_plus		{ owb::Transaction* getTx(); API };
namespace o_api_owb_plus_plus	{ owb::Transaction* getTx(); API };

namespace o_api_stmlite		{ GET_TX API };

namespace o_api_undolog_invis	 		{ GET_TX API };
namespace o_api_undolog_invis_ordered 	{ GET_TX API };

namespace o_api_undolog_vis	 			{ undolog_vis::Transaction* getTx(); API };
namespace o_api_undolog_vis_ordered 	{ undolog_vis::Transaction* getTx(); API };

namespace o_api_oul_speculative 			{ oul_speculative::Transaction* getTx(); API };
namespace o_api_oul_speculative_plus 		{ oul_speculative::Transaction* getTx(); API };
namespace o_api_oul_speculative_plus_plus	{ oul_speculative::Transaction* getTx(); API };
namespace o_api_oul_steal			 		{ oul_steal::Transaction* getTx(); API };
namespace o_api_oul_steal_plus		 		{ oul_steal::Transaction* getTx(); API };
namespace o_api_oul_steal_plus_plus	 		{ oul_steal::Transaction* getTx(); API };

#endif
