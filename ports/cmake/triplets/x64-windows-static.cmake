set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)

# Protobuf 36 selects the empty-string symbol's type from the C++ language mode.
# Keep its runtime and Abseil in the same mode as the C++20 application.
if(PORT STREQUAL "protobuf" OR PORT STREQUAL "abseil")
    list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS "-DCMAKE_CXX_STANDARD=20")
endif()
