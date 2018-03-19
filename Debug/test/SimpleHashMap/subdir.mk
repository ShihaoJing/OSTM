################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/SimpleHashMap/hashmap.cpp 

OBJS += \
./test/SimpleHashMap/hashmap.o 

CPP_DEPS += \
./test/SimpleHashMap/hashmap.d 


# Each subdirectory must supply rules for building sources it contributes
test/SimpleHashMap/%.o: ../test/SimpleHashMap/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


