#ifndef _RTM_H
#define _RTM_H 1

/*
 * Copyright (c) 2012,2013 Intel Corporation
 * Author: Andi Kleen
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that: (1) source code distributions
 * retain the above copyright notice and this paragraph in its entirety, (2)
 * distributions including binary code include the above copyright notice and
 * this paragraph in its entirety in the documentation or other materials
 * provided with the distribution
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

/* Official RTM intrinsics interface matching gcc/icc, but works
   on older gcc compatible compilers and binutils. */

#ifdef HTM

#include <immintrin.h>
#include <rtmintrin.h>

#else

#define _XBEGIN_STARTED		(~0u)
#define _XABORT_EXPLICIT	(1 << 0)
#define _XABORT_RETRY		(1 << 1)
#define _XABORT_CONFLICT	(1 << 2)
#define _XABORT_CAPACITY	(1 << 3)
#define _XABORT_DEBUG		(1 << 4)
#define _XABORT_NESTED		(1 << 5)
#define _XABORT_CODE(x)     (((x) >> 24) & 0xFF)

#define __rtm_force_inline __attribute__((__always_inline__)) inline

static __rtm_force_inline int _xbegin(void)
{
	return _XABORT_EXPLICIT;
}

static __rtm_force_inline void _xend(void)
{
}

static __rtm_force_inline void _xabort(const unsigned int status)
{
}

static __rtm_force_inline int _xtest(void)
{
	unsigned char out;
	return out;
}
#endif

#endif
