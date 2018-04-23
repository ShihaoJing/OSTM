################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/quake/quake.cpp 

OBJS += \
./test/quake/quake.o 

CPP_DEPS += \
./test/quake/quake.d 


# Each subdirectory must supply rules for building sources it contributes
test/quake/%.o: ../test/quake/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


