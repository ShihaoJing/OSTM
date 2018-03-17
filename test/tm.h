
#  include <assert.h>
#include "../conf.h"
#include "../o-api/o-api.hpp"

#  define TM_ARG                        /* nothing */
#  define TM_ARG_ALONE                  /* nothing */
#  define TM_ARGDECL                    /* nothing */
#  define TM_ARGDECL_ALONE              /* nothing */
#  define TM_CALLABLE                   /* nothing */

#  define TM_STARTUP(numThread)         /* nothing */
#  define TM_SHUTDOWN()                 /* nothing */

#  define TM_THREAD_ENTER()             /* nothing */
#  define TM_THREAD_EXIT()              /* nothing */

#  ifdef SIMULATOR

#    include "thread.h"

#    define P_MALLOC(size)              memory_get(thread_getId(), size)
#    define P_FREE(ptr)                 /* TODO: thread local free is non-trivial */
#    define TM_MALLOC(size)             memory_get(thread_getId(), size)
#    define TM_FREE(ptr)                /* TODO: thread local free is non-trivial */

#  else /* !SIMULATOR */

#    define P_MALLOC(size)              malloc(size)
#    define P_FREE(ptr)                 //free(ptr)
#    define TM_MALLOC(size)             malloc(size)
#    define TM_FREE(ptr)                //free(ptr)

#  endif /* !SIMULATOR */

#  define TM_BEGIN()                    o_api_lock::cs_start(); /* nothing */
#  define TM_BEGIN_RO()                 o_api_lock::cs_start(); /* nothing */
#  define TM_END()                      o_api_lock::cs_end(); /* nothing */
#  define TM_RESTART()                  assert(0)

#  define TM_SHARED_READ_I(var)         (var)
#  define TM_SHARED_READ_L(var)         (var)
#  define TM_SHARED_READ_P(var)         (var)
#  define TM_SHARED_READ_F(var)         (var)

#  define TM_SHARED_WRITE_I(var, val)   ({var = val; var;})
#  define TM_SHARED_WRITE_L(var, val)   ({var = val; var;})
#  define TM_SHARED_WRITE_P(var, val)   ({var = val; var;})
#  define TM_SHARED_WRITE_F(var, val)   ({var = val; var;})

#  define TM_LOCAL_WRITE_I(var, val)    ({var = val; var;})
#  define TM_LOCAL_WRITE_L(var, val)    ({var = val; var;})
#  define TM_LOCAL_WRITE_P(var, val)    ({var = val; var;})
#  define TM_LOCAL_WRITE_F(var, val)    ({var = val; var;})

#  define SEQ_MALLOC(size)              malloc(size)
#  define SEQ_FREE(ptr)                 free(ptr)

//#  define MAIN(argc, argv)              int main (int argc, char** argv)
#  define MAIN_RETURN(val)              O_API::print_statistics(); return val

#  define GOTO_SIM()                    /* nothing */
#  define GOTO_REAL()                   /* nothing */
#  define IS_IN_SIM()                   (0)

#  define SIM_GET_NUM_CPU(var)          /* nothing */

#  define TM_PRINTF                     printf
#  define TM_PRINT0                     printf
#  define TM_PRINT1                     printf
#  define TM_PRINT2                     printf
#  define TM_PRINT3                     printf

#  define TM_BEGIN_WAIVER()
#  define TM_END_WAIVER()

#  define P_MEMORY_STARTUP(numThread)   /* nothing */
#  define P_MEMORY_SHUTDOWN()           /* nothing */

#  define TM_EARLY_RELEASE(var)         /* nothing */

// --------------------------------------------------------------------------------------------------- //

#  define OTM_ARG                        tx,
#  define OTM_ARG_ALONE                  tx
#  define OTM_ARGDECL                     TRANSACTION* tx,
#  define OTM_ARGDECL_ALONE               TRANSACTION* tx

#  define OTM_BEGIN()                    TRANSACTION* tx = O_API::getTx();/* nothing */
#  define OTM_END()                      /* nothing */
#  define OTM_RESTART()                  tx->suicide(0);

#  define OTM_SHARED_READ_I(var)         (tx->read_i(&(var)))
#  define OTM_SHARED_READ_L(var)         (tx->read_l(&var))
#  define OTM_SHARED_READ_P(var)         (tx->read_t(&(var)))
#  define OTM_SHARED_READ_F(var)         (tx->read_f(&var))

#  define OTM_SHARED_WRITE_I(var, val)   ({tx->write_i(&var, val);})
#  define OTM_SHARED_WRITE_L(var, val)   ({tx->write_l(&var, val);})
#  define OTM_SHARED_WRITE_P(var, val)   ({tx->write_t(&(var), val);})
#  define OTM_SHARED_WRITE_F(var, val)   ({tx->write_f(&var, val);})

//#  define OTM_SHARED_READ_I(var)         (var)
//#  define OTM_SHARED_READ_L(var)         (var)
//#  define OTM_SHARED_READ_P(var)         (var)
//#  define OTM_SHARED_READ_F(var)         (var)
//
//#  define OTM_SHARED_WRITE_I(var, val)   ({var = val; var;})
//#  define OTM_SHARED_WRITE_L(var, val)   ({var = val; var;})
//#  define OTM_SHARED_WRITE_P(var, val)   ({var = val; var;})
//#  define OTM_SHARED_WRITE_F(var, val)   ({var = val; var;})


