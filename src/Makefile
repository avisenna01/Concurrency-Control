# Set the compiler and compile-time loaded libraries
CXX           := g++
CXXFLAGS := -g -Wall -Werror

LDFLAGS := -lpthread

all:
	$(CXX) simple.cpp -o simple $(CXXFLAGS) $(LDFLAGS)
	$(CXX) OCC.cpp -o OCC $(CXXFLAGS) $(LDFLAGS)
	$(CXX) MVCC.cpp -o MVCC $(CXXFLAGS) $(LDFLAGS)

simple:
	./simple

occ:
	./OCC

mvcc:
	./MVCC performance

clean:
	rm -f simple OCC MVCC

.PHONY: all simple occ mvcc