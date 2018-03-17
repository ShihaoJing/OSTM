//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _DUMMY_
#define _DUMMY_

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

namespace dummy {

class Transaction : public commons::AbstractTransaction{
	jmp_buf* buff;
public:

	void start(jmp_buf* buff){
		this->buff = buff;
	}

	void* read(void** so) {
		return *so;
	}

	void write(void** so, void* value) {
		*so = value;
	}

	bool abort() {
		LOG("Abort");
		if(buff)
			longjmp (*buff, 1);
		return true;
	}

	bool commit() {
		return true;
	}

	void suicide(int reason){
	}
};

}

#endif
