cmake_minimum_required(VERSION 3.16)
project(mmapq_consumer C)

# Consumption smoke test: find_package(mmapq) against a staged install, then
# link mmapq::mmapq. Build with:
#   cmake -S test/consumer -B build-consumer -Dmmapq_DIR=<stage>/lib/cmake/mmapq
find_package(mmapq REQUIRED)

add_executable(consumer main.c)
target_link_libraries(consumer PRIVATE mmapq::mmapq)
