include_guard(GLOBAL)

function(iot_engine_configure_target target)
    if(MSVC)
        target_compile_options("${target}" PRIVATE /utf-8 /Zc:__cplusplus)
    endif()
endfunction()

function(iot_engine_add_test name)
    set(options LARGE NO_PROJECT_INCLUDE)
    set(multi_value_args
        SOURCES
        LIBRARIES
        DEFINITIONS
        MSVC_DEFINITIONS)
    cmake_parse_arguments(PARSE_ARGV 1 arg
        "${options}" "" "${multi_value_args}")
    if(arg_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR
            "Unknown arguments for ${name}-test: ${arg_UNPARSED_ARGUMENTS}")
    endif()
    if(NOT arg_SOURCES)
        message(FATAL_ERROR "${name}-test requires at least one source")
    endif()

    set(target "${name}-test")
    add_executable("${target}" ${arg_SOURCES})
    if(NOT arg_NO_PROJECT_INCLUDE)
        target_include_directories("${target}" PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}")
    endif()
    if(arg_LIBRARIES)
        target_link_libraries("${target}" PRIVATE ${arg_LIBRARIES})
    endif()
    if(arg_DEFINITIONS)
        target_compile_definitions("${target}" PRIVATE ${arg_DEFINITIONS})
    endif()

    iot_engine_configure_target("${target}")
    if(MSVC)
        if(arg_LARGE)
            target_compile_options("${target}" PRIVATE /bigobj /FS)
        endif()
        if(arg_MSVC_DEFINITIONS)
            target_compile_definitions("${target}" PRIVATE
                ${arg_MSVC_DEFINITIONS})
        endif()
    endif()
    add_test(NAME "${name}" COMMAND "${target}")
endfunction()

# Integration tests run tests/*.ts with Bun against disposable fixtures driven
# by ports/cmake/integration-fixture.cmake. A fixture owns one database and one
# Redis database index on the servers named by IOT_TEST_POSTGRES_URL and
# IOT_TEST_REDIS_URL, plus a block of ten loopback ports for its iot-engine
# processes. Without those variables the tests report as skipped.
set(IOT_ENGINE_INTEGRATION_PORT_BASE 25100 CACHE STRING
    "First loopback port of the integration fixture port blocks")

# iot_engine_add_integration_fixture(<name> [MANAGED] [GB28181] [SEED_UPLOADS]
#     [INSTANCES <n>] [SETTINGS <KEY=VALUE>...])
#   MANAGED       each test starts and stops iot-engine itself; tests serialize
#   GB28181       enable the SIP and media runtime; SIP uses the block's last port,
#                 WebRTC the one before it
#   SEED_UPLOADS  place abandoned upload files before the first start
#   INSTANCES     independent iot-engine processes on consecutive ports
#   SETTINGS      .env overrides for every instance
function(iot_engine_add_integration_fixture name)
    cmake_parse_arguments(PARSE_ARGV 1 arg
        "MANAGED;GB28181;SEED_UPLOADS" "INSTANCES" "SETTINGS")
    if(arg_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR
            "Unknown arguments for integration fixture ${name}: ${arg_UNPARSED_ARGUMENTS}")
    endif()
    if(NOT arg_INSTANCES)
        set(arg_INSTANCES 1)
    endif()
    get_property(index GLOBAL PROPERTY IOT_ENGINE_INTEGRATION_FIXTURES)
    list(LENGTH index index)
    # Redis database 0 stays untouched; the default server provides 16.
    math(EXPR redis_database "${index} + 1")
    if(redis_database GREATER 15)
        message(FATAL_ERROR "Integration fixtures exceed the 15 Redis databases available")
    endif()
    math(EXPR api_port "${IOT_ENGINE_INTEGRATION_PORT_BASE} + 10 * ${index}")
    set_property(GLOBAL APPEND PROPERTY IOT_ENGINE_INTEGRATION_FIXTURES "${name}")

    foreach(option MANAGED GB28181 SEED_UPLOADS)
        if(arg_${option})
            set(value_${option} ON)
        else()
            set(value_${option} OFF)
        endif()
    endforeach()
    set(directory "${CMAKE_BINARY_DIR}/integration/${name}")
    file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/integration/${name}-$<CONFIG>.cmake"
        CONTENT "set(FIXTURE \"${name}\")
set(FIXTURE_DIRECTORY \"${directory}\")
set(SOURCE_DIR \"${PROJECT_SOURCE_DIR}\")
set(BUN \"${BUN_EXECUTABLE}\")
set(ENGINE \"$<TARGET_FILE:iot-engine>\")
set(API_PORT ${api_port})
set(REDIS_DATABASE ${redis_database})
set(INSTANCES ${arg_INSTANCES})
set(MANAGED ${value_MANAGED})
set(GB28181 ${value_GB28181})
set(SEED_UPLOADS ${value_SEED_UPLOADS})
set(SETTINGS \"${arg_SETTINGS}\")
")
    set(configuration
        "-DFIXTURE_CONFIG=${CMAKE_BINARY_DIR}/integration/${name}-$<CONFIG>.cmake")
    set(script -P "${PROJECT_SOURCE_DIR}/ports/cmake/integration-fixture.cmake")
    set(fixture_tests "integration-${name}-stop")
    add_test(NAME "integration-${name}-stop"
        COMMAND "${CMAKE_COMMAND}" ${configuration} -DACTION=stop ${script})
    set_tests_properties("integration-${name}-stop" PROPERTIES
        FIXTURES_CLEANUP "iot-${name}")
    if(NOT arg_MANAGED)
        add_test(NAME "integration-${name}-start"
            COMMAND "${CMAKE_COMMAND}" ${configuration} -DACTION=start ${script})
        set_tests_properties("integration-${name}-start" PROPERTIES
            FIXTURES_SETUP "iot-${name}" TIMEOUT 120)
        list(APPEND fixture_tests "integration-${name}-start")
    endif()
    set_tests_properties(${fixture_tests} PROPERTIES
        LABELS integration
        RESOURCE_LOCK "iot-${name}"
        SKIP_RETURN_CODE 77)
endfunction()

# iot_engine_add_integration(<stem> [FIXTURE <name>] [MQTT] [RUN_SERIAL]
#     [MIGRATION_FAILURE <regex>] [EXECUTABLE <path>] [TIMEOUT <seconds>]
#     [ENVIRONMENT <NAME=VALUE>...])
# Registers tests/<stem>.ts as CTest test <stem> labelled `integration`.
# MQTT tests also need the broker named by IOT_TEST_MQTT_URL. RUN_SERIAL tests
# change server-wide Redis state (ACL) and run while no other test runs.
# EXECUTABLE is the native program under test, passed as the script argument;
# a fixture test whose EXECUTABLE is absent reports as skipped.
# MIGRATION_FAILURE drives the migration phase protocol in a MANAGED fixture;
# the regex must match the rejected startup.
function(iot_engine_add_integration stem)
    cmake_parse_arguments(PARSE_ARGV 1 arg
        "MQTT;RUN_SERIAL" "FIXTURE;MIGRATION_FAILURE;EXECUTABLE;TIMEOUT" "ENVIRONMENT")
    if(arg_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR
            "Unknown arguments for integration test ${stem}: ${arg_UNPARSED_ARGUMENTS}")
    endif()
    set(script "tests/${stem}.ts")
    if(NOT EXISTS "${PROJECT_SOURCE_DIR}/${script}")
        message(FATAL_ERROR "Integration test ${script} does not exist")
    endif()
    if(NOT arg_TIMEOUT)
        set(arg_TIMEOUT 300)
    endif()
    if(arg_FIXTURE)
        get_property(fixtures GLOBAL PROPERTY IOT_ENGINE_INTEGRATION_FIXTURES)
        if(NOT arg_FIXTURE IN_LIST fixtures)
            message(FATAL_ERROR "Integration fixture ${arg_FIXTURE} is not registered")
        endif()
        set(command "${CMAKE_COMMAND}"
            "-DFIXTURE_CONFIG=${CMAKE_BINARY_DIR}/integration/${arg_FIXTURE}-$<CONFIG>.cmake"
            -DACTION=run
            "-DSCRIPT=${script}")
        if(DEFINED arg_MIGRATION_FAILURE)
            list(APPEND command "-DMIGRATION_FAILURE=${arg_MIGRATION_FAILURE}")
        endif()
        if(arg_EXECUTABLE)
            list(APPEND command "-DEXECUTABLE=${arg_EXECUTABLE}")
        endif()
        if(arg_MQTT)
            list(APPEND command -DMQTT=ON)
        endif()
        list(APPEND command -P "${PROJECT_SOURCE_DIR}/ports/cmake/integration-fixture.cmake")
        add_test(NAME "${stem}" COMMAND ${command})
        set_tests_properties("${stem}" PROPERTIES
            FIXTURES_REQUIRED "iot-${arg_FIXTURE}"
            RESOURCE_LOCK "iot-${arg_FIXTURE}")
    else()
        add_test(NAME "${stem}"
            COMMAND "${BUN_EXECUTABLE}" run "${PROJECT_SOURCE_DIR}/${script}" ${arg_EXECUTABLE}
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
    endif()
    set_tests_properties("${stem}" PROPERTIES
        LABELS integration
        SKIP_RETURN_CODE 77
        TIMEOUT ${arg_TIMEOUT})
    if(arg_RUN_SERIAL)
        set_tests_properties("${stem}" PROPERTIES RUN_SERIAL ON)
    endif()
    if(arg_ENVIRONMENT)
        set_tests_properties("${stem}" PROPERTIES ENVIRONMENT "${arg_ENVIRONMENT}")
    endif()
endfunction()
