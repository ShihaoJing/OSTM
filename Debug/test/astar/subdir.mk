################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../test/astar/CreateWay_.cpp \
../test/astar/Places_.cpp \
../test/astar/Random.cpp \
../test/astar/RegBounds_.cpp \
../test/astar/RegMng_.cpp \
../test/astar/RegWay_.cpp \
../test/astar/Region_.cpp \
../test/astar/Way2_.cpp \
../test/astar/WayInit_.cpp \
../test/astar/Way_.cpp \
../test/astar/main.cpp 

OBJS += \
./test/astar/CreateWay_.o \
./test/astar/Places_.o \
./test/astar/Random.o \
./test/astar/RegBounds_.o \
./test/astar/RegMng_.o \
./test/astar/RegWay_.o \
./test/astar/Region_.o \
./test/astar/Way2_.o \
./test/astar/WayInit_.o \
./test/astar/Way_.o \
./test/astar/main.o 

CPP_DEPS += \
./test/astar/CreateWay_.d \
./test/astar/Places_.d \
./test/astar/Random.d \
./test/astar/RegBounds_.d \
./test/astar/RegMng_.d \
./test/astar/RegWay_.d \
./test/astar/Region_.d \
./test/astar/Way2_.d \
./test/astar/WayInit_.d \
./test/astar/Way_.d \
./test/astar/main.d 


# Each subdirectory must supply rules for building sources it contributes
test/astar/%.o: ../test/astar/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


