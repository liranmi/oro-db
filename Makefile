# Top-level convenience wrapper around CMake
# Usage:
#   make debug -sj        # Debug build of oro_bench
#   make release -sj      # Release build of oro_bench
#   make debug-all -sj    # Debug build of everything
#   make clean             # Remove all build artifacts

BUILD_DIR_DEBUG   = build/debug
BUILD_DIR_RELEASE = build/release
NPROC            := $(shell nproc)

.PHONY: debug release debug-all release-all sqlite clean

debug:
	@cmake -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON > /dev/null
	$(MAKE) -C $(BUILD_DIR_DEBUG) -j$(NPROC) oro_bench

release:
	@cmake -S . -B $(BUILD_DIR_RELEASE) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON > /dev/null
	$(MAKE) -C $(BUILD_DIR_RELEASE) -j$(NPROC) oro_bench

debug-all:
	@cmake -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON > /dev/null
	$(MAKE) -C $(BUILD_DIR_DEBUG) -j$(NPROC)

release-all:
	@cmake -S . -B $(BUILD_DIR_RELEASE) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON > /dev/null
	$(MAKE) -C $(BUILD_DIR_RELEASE) -j$(NPROC)

sqlite:
	@cmake -S . -B $(BUILD_DIR_DEBUG) -DCMAKE_BUILD_TYPE=Debug -DORO_BUILD_SQLITE=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON > /dev/null
	$(MAKE) -C $(BUILD_DIR_DEBUG) -j$(NPROC) oro_test_sqlite oro_test_mot_engine oro_shell

clean:
	rm -rf build
