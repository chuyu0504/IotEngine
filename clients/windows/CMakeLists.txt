cmake_minimum_required(VERSION 3.25)

project(iot_egine_windows VERSION 0.7.4 LANGUAGES CXX RC)

if(NOT WIN32)
  message(FATAL_ERROR "The Windows client must be configured on Windows.")
endif()

set(IOT_EGINE_WINDOWS_VERSION "0.7.4" CACHE STRING "Windows client version")
set(IOT_EGINE_WINDOWS_SOURCE_COMMIT "" CACHE STRING "Verified source snapshot commit; defaults to repository HEAD")
set(IOT_EGINE_WINDOWS_REPOSITORY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
get_filename_component(IOT_EGINE_WINDOWS_REPOSITORY_ROOT
  "${IOT_EGINE_WINDOWS_REPOSITORY_ROOT}" ABSOLUTE)
set(IOT_EGINE_WINDOWS_BUILD_ROOT "${IOT_EGINE_WINDOWS_REPOSITORY_ROOT}/build")
set(IOT_EGINE_WINDOWS_NATIVE_BUILD "${CMAKE_BINARY_DIR}")
set(IOT_EGINE_WINDOWS_NATIVE_OUTPUT
  "${IOT_EGINE_WINDOWS_BUILD_ROOT}/windows-vpn-native/output")
set(IOT_EGINE_WINDOWS_WINUI_OUTPUT
  "${IOT_EGINE_WINDOWS_BUILD_ROOT}/windows-winui/Release")
set(IOT_EGINE_WINDOWS_PACKAGE_OUTPUT
  "${IOT_EGINE_WINDOWS_BUILD_ROOT}/iot-egine-package")
set(IOT_EGINE_WINDOWS_INSTALLER_OUTPUT
  "${IOT_EGINE_WINDOWS_BUILD_ROOT}/iot-egine-Setup-${IOT_EGINE_WINDOWS_VERSION}-x64.exe")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/$<CONFIG>")

enable_testing()
if(NOT WIN32 OR NOT MSVC)
  message(FATAL_ERROR "The Windows client requires MSVC 2022 on Windows x64")
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(/utf-8 /W4 /permissive- /EHsc)
add_compile_definitions(UNICODE _UNICODE NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0A00)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

add_library(vpn_common STATIC common/text.cpp common/files.cpp common/json.cpp common/data_protection.cpp common/ipc.cpp)
target_include_directories(vpn_common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/common" "${CMAKE_CURRENT_SOURCE_DIR}/vendor")
target_link_libraries(vpn_common PUBLIC advapi32 crypt32 shell32 ole32 ws2_32 normaliz iphlpapi)
set_target_properties(vpn_common PROPERTIES MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

file(GLOB service_sources CONFIGURE_DEPENDS "service/*.cpp")
list(FILTER service_sources EXCLUDE REGEX "/main.cpp$")
add_library(vpn_service_runtime STATIC ${service_sources})
target_link_libraries(vpn_service_runtime PUBLIC vpn_common winhttp)
add_executable(vpn_service service/main.cpp assets/product.rc)
target_link_libraries(vpn_service PRIVATE vpn_service_runtime)
set_target_properties(vpn_service PROPERTIES OUTPUT_NAME "IotVpn.Service")

file(GLOB control_sources CONFIGURE_DEPENDS "service_control/*.cpp")
add_executable(vpn_service_control WIN32 ${control_sources} assets/product.rc)
target_link_libraries(vpn_service_control PRIVATE vpn_common wtsapi32 wintrust crypt32)
set_target_properties(vpn_service_control PROPERTIES OUTPUT_NAME "IotVpn.ServiceControl")

add_executable(installer_snapshot_tests tests/installer_snapshot_tests.cpp)
target_link_libraries(installer_snapshot_tests PRIVATE vpn_common)
add_test(NAME installer-snapshot COMMAND installer_snapshot_tests)
add_executable(vpn_transport_probe tests/transport_probe.cpp)
target_link_libraries(vpn_transport_probe PRIVATE vpn_service_runtime)
add_executable(vpn_credential_tests tests/credential_tests.cpp)
target_link_libraries(vpn_credential_tests PRIVATE vpn_common)
add_test(NAME vpn-credentials COMMAND vpn_credential_tests)
add_executable(vpn_service_tests tests/service_tests.cpp)
target_link_libraries(vpn_service_tests PRIVATE vpn_service_runtime)
add_test(NAME vpn-service COMMAND vpn_service_tests)
add_test(NAME vpn-sse-retry COMMAND vpn_service_tests --retry-boundary)
set_tests_properties(vpn-sse-retry PROPERTIES TIMEOUT 210)
add_executable(vpn_common_tests tests/common_tests.cpp)
target_link_libraries(vpn_common_tests PRIVATE vpn_service_runtime)
add_test(NAME vpn-common COMMAND vpn_common_tests)
add_executable(vpn_gui_tests tests/gui_controller_tests.cpp)
target_link_libraries(vpn_gui_tests PRIVATE vpn_common)
add_test(NAME vpn-gui COMMAND vpn_gui_tests)
add_test(
  NAME wireguard-source-fetch
  COMMAND "${CMAKE_COMMAND}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/wireguard_source_fetch_test.cmake")

add_executable(vpn_platform_http_probe tests/platform_http_probe.cpp)
target_link_libraries(vpn_platform_http_probe PRIVATE vpn_service_runtime)

set(_wireguard_files
  "${IOT_EGINE_WINDOWS_NATIVE_OUTPUT}/tunnel.dll"
  "${IOT_EGINE_WINDOWS_NATIVE_OUTPUT}/wireguard.dll")
add_custom_command(
  OUTPUT ${_wireguard_files}
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DLOCK_FILE=${CMAKE_CURRENT_SOURCE_DIR}/wireguard.lock.json"
    "-P" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWireGuard.cmake"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWireGuard.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/FetchPinnedGit.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/wireguard.lock.json"
  VERBATIM)
add_custom_target(windows-wireguard DEPENDS ${_wireguard_files})

set(_winui_output "${IOT_EGINE_WINDOWS_WINUI_OUTPUT}/iot-egine.exe")
add_custom_command(
  OUTPUT "${_winui_output}"
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DLOCK_FILE=${CMAKE_CURRENT_SOURCE_DIR}/winui/packages.lock.json"
    "-P" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWinUI.cmake"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWinUI.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/packages.lock.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/iot-egine.vcxproj"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/app.manifest"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/main.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/connection_controller.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/text.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/text.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/files.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/files.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/json.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/json.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/data_protection.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/data_protection.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/ipc.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/ipc.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/common/product.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/connection_controller.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/credentials.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/assets/product.rc"
    "${CMAKE_CURRENT_SOURCE_DIR}/assets/product.ico"
    "${CMAKE_CURRENT_SOURCE_DIR}/assets/product.svg"
  VERBATIM)
add_custom_target(windows-winui DEPENDS "${_winui_output}")

add_custom_target(windows-native-runtime-files
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/$<CONFIG>"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${IOT_EGINE_WINDOWS_NATIVE_OUTPUT}/tunnel.dll"
    "${CMAKE_BINARY_DIR}/$<CONFIG>/tunnel.dll"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
    "${IOT_EGINE_WINDOWS_NATIVE_OUTPUT}/wireguard.dll"
    "${CMAKE_BINARY_DIR}/$<CONFIG>/wireguard.dll"
  DEPENDS windows-wireguard vpn_service vpn_service_control
  VERBATIM)

add_custom_target(windows-native-tests
  COMMAND "${CMAKE_CTEST_COMMAND}"
    --test-dir "${CMAKE_BINARY_DIR}"
    -C "$<CONFIG>"
    --output-on-failure
  DEPENDS
    windows-native-runtime-files
    installer_snapshot_tests
    vpn_transport_probe
    vpn_credential_tests
    vpn_service_tests
    vpn_common_tests
    vpn_gui_tests
  VERBATIM)

add_custom_command(
  OUTPUT "${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}/manifest.json"
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DNATIVE_BUILD_DIRECTORY=${IOT_EGINE_WINDOWS_NATIVE_BUILD}"
    "-DNATIVE_OUTPUT=${IOT_EGINE_WINDOWS_NATIVE_OUTPUT}"
    "-DWINUI_OUTPUT=${IOT_EGINE_WINDOWS_WINUI_OUTPUT}"
    "-DPACKAGE_DIRECTORY=${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}"
    "-DCLIENT_CONFIG=$<CONFIG>"
    "-DVERSION=${IOT_EGINE_WINDOWS_VERSION}"
    "-DLOCK_FILE=${CMAKE_CURRENT_SOURCE_DIR}/wireguard.lock.json"
    "-DWINUI_LOCK_FILE=${CMAKE_CURRENT_SOURCE_DIR}/winui/packages.lock.json"
    "-P" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageWindowsClient.cmake"
  DEPENDS
    windows-wireguard
    windows-winui
    windows-native-tests
    ${_wireguard_files}
    ${_winui_output}
    vpn_service
    vpn_service_control
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageWindowsClient.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
    "${CMAKE_CURRENT_SOURCE_DIR}/wireguard.lock.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/winui/packages.lock.json"
  VERBATIM)
add_custom_target(windows-package
  DEPENDS "${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}/manifest.json")

add_custom_command(
  OUTPUT "${IOT_EGINE_WINDOWS_INSTALLER_OUTPUT}"
    "${IOT_EGINE_WINDOWS_BUILD_ROOT}/windows-client-downloads/iot-egine-Setup-x64.exe"
    "${IOT_EGINE_WINDOWS_BUILD_ROOT}/windows-client-downloads/iot-egine-Setup-x64.exe.sha256"
    "${IOT_EGINE_WINDOWS_BUILD_ROOT}/windows-client-downloads/windows-client.json"
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DPACKAGE_DIRECTORY=${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}"
    "-DOUTPUT_FILE=${IOT_EGINE_WINDOWS_INSTALLER_OUTPUT}"
    "-DVERSION=${IOT_EGINE_WINDOWS_VERSION}"
    "-DSKIP_PACKAGE_BUILD=ON"
    "-DSOURCE_COMMIT=${IOT_EGINE_WINDOWS_SOURCE_COMMIT}"
    "-P" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWindowsInstaller.cmake"
  DEPENDS
    windows-package
    "${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}/manifest.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWindowsInstaller.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/installer/Package.wxs"
    "${CMAKE_CURRENT_SOURCE_DIR}/installer/Bundle.wxs"
    "${CMAKE_CURRENT_SOURCE_DIR}/assets/product.ico"
  VERBATIM)
add_custom_target(windows-installer
  DEPENDS "${IOT_EGINE_WINDOWS_INSTALLER_OUTPUT}")

add_custom_target(windows-installer-from-package
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DPACKAGE_DIRECTORY=${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}"
    "-DOUTPUT_FILE=${IOT_EGINE_WINDOWS_INSTALLER_OUTPUT}"
    "-DVERSION=${IOT_EGINE_WINDOWS_VERSION}"
    "-DSKIP_PACKAGE_BUILD=ON"
    "-DSOURCE_COMMIT=${IOT_EGINE_WINDOWS_SOURCE_COMMIT}"
    "-P" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWindowsInstaller.cmake"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildWindowsInstaller.cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/installer/Package.wxs"
    "${CMAKE_CURRENT_SOURCE_DIR}/installer/Bundle.wxs"
    "${CMAKE_CURRENT_SOURCE_DIR}/assets/product.ico"
  VERBATIM)

add_custom_target(windows-client ALL DEPENDS windows-package)

add_custom_target(windows-installer-lifecycle-test
  COMMAND "${CMAKE_COMMAND}"
    "-DBUILD_ROOT=${IOT_EGINE_WINDOWS_BUILD_ROOT}"
    "-DSOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DPACKAGE_DIRECTORY=${IOT_EGINE_WINDOWS_PACKAGE_OUTPUT}"
    "-DOUTPUT_FILE=${IOT_EGINE_WINDOWS_INSTALLER_OUTPUT}"
    "-DVERSION=${IOT_EGINE_WINDOWS_VERSION}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TestWindowsInstaller.cmake"
  DEPENDS windows-installer
  VERBATIM)

message(STATUS "Windows client targets: windows-client, windows-package, windows-installer, windows-installer-from-package")
# The native HTTP/SSE probe uses the same pinned Bun runtime as the frontend fixture.
set(IOT_EGINE_WINDOWS_BUN
  "${IOT_EGINE_WINDOWS_BUILD_ROOT}/bun-1.3.14/bun-windows-x64/bun.exe"
  CACHE FILEPATH "Pinned Bun executable for Windows HTTP/SSE integration")
add_custom_target(windows-http-sse-test
  COMMAND "${IOT_EGINE_WINDOWS_BUN}"
    "${IOT_EGINE_WINDOWS_REPOSITORY_ROOT}/tests/windows-http-sse-integration.ts"
    "$<TARGET_FILE:vpn_platform_http_probe>"
  DEPENDS vpn_platform_http_probe
  WORKING_DIRECTORY "${IOT_EGINE_WINDOWS_REPOSITORY_ROOT}"
  VERBATIM)

add_dependencies(windows-native-tests windows-http-sse-test)
