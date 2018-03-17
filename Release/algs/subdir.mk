################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../algs/oul_kill.cpp \
../algs/oul_speculative.cpp \
../algs/oul_steal.cpp \
../algs/owb.cpp \
../algs/owbage.cpp \
../algs/owbrh.cpp \
../algs/stmlite.cpp \
../algs/tl2.cpp \
../algs/undolog_invis.cpp \
../algs/undolog_vis.cpp 

OBJS += \
./algs/oul_kill.o \
./algs/oul_speculative.o \
./algs/oul_steal.o \
./algs/owb.o \
./algs/owbage.o \
./algs/owbrh.o \
./algs/stmlite.o \
./algs/tl2.o \
./algs/undolog_invis.o \
./algs/undolog_vis.o 

CPP_DEPS += \
./algs/oul_kill.d \
./algs/oul_speculative.d \
./algs/oul_steal.d \
./algs/owb.d \
./algs/owbage.d \
./algs/owbrh.d \
./algs/stmlite.d \
./algs/tl2.d \
./algs/undolog_invis.d \
./algs/undolog_vis.d 


# Each subdirectory must supply rules for building sources it contributes
algs/%.o: ../algs/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


