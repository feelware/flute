CXX := mpic++
CC := gcc
CXXFLAGS := -O3
CFLAGS := -O2

# avoid tab-vs-space errors in recipes
.RECIPEPREFIX := >

OPENCV_CFLAGS := $(shell pkg-config --cflags opencv4)
OPENCV_LIBS := $(shell pkg-config --cflags --libs opencv4)

PROCESS_VIDEO := process_video
RABBIT := rabbitmq_consumer
OBJS := download_media.o apply_lut.o

all: $(PROCESS_VIDEO) $(RABBIT)

download_media.o: download_media.cpp download_media.h
>$(CXX) $(CXXFLAGS) -c download_media.cpp -o $@ -lcurl -lpthread

apply_lut.o: apply_lut.cpp apply_lut.h
>$(CXX) $(CXXFLAGS) -c apply_lut.cpp -o $@ $(OPENCV_CFLAGS)

$(PROCESS_VIDEO): process_video.cpp $(OBJS)
>$(CXX) $(CXXFLAGS) -o $@ process_video.cpp $(OBJS) -lcurl -lpthread $(OPENCV_LIBS)

$(RABBIT): rabbitmq_consumer.c
>$(CC) $(CFLAGS) -o $@ rabbitmq_consumer.c -lrabbitmq -lcjson

clean:
>rm -f $(PROCESS_VIDEO) $(RABBIT) $(OBJS)

.PHONY: all clean
