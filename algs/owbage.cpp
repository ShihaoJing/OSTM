#include "owbage.hpp"

namespace owbage{

	Lock locks[LOCK_SIZE];
	class Transaction* current[MAX_CURRENT];

}
