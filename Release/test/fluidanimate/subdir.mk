################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/fluidanimate/cellpool.cpp \
../test/fluidanimate/fluidcmp.cpp \
../test/fluidanimate/main.cpp \
../test/fluidanimate/parsec_barrier.cpp 

OBJS += \
./test/fluidanimate/cellpool.o \
./test/fluidanimate/fluidcmp.o \
./test/fluidanimate/main.o \
./test/fluidanimate/parsec_barrier.o 

CPP_DEPS += \
./test/fluidanimate/cellpool.d \
./test/fluidanimate/fluidcmp.d \
./test/fluidanimate/main.d \
./test/fluidanimate/parsec_barrier.d 


# Each subdirectory must supply rules for building sources it contributes
test/fluidanimate/%.o: ../test/fluidanimate/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


