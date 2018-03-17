#include "owbrh.hpp"

namespace owbrh{

	int htm_count = 0;
	int htm_fail_count = 0;

	Lock locks[LOCK_SIZE];
	class Transaction* current[MAX_CURRENT];

}
