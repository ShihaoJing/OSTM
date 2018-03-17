################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/blackscholes/blackscholes.cpp 

OBJS += \
./test/blackscholes/blackscholes.o 

CPP_DEPS += \
./test/blackscholes/blackscholes.d 


# Each subdirectory must supply rules for building sources it contributes
test/blackscholes/%.o: ../test/blackscholes/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


