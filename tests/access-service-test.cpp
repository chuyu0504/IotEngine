#include <iostream>
#include <memory_resource>
#include <stdexcept>
#include <string>

#include "service/modules/open_access/open_access.service.h"

#include "tests/database.fixture.h"

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void requirePublicHistoryQueryShape() {
    const auto statement = test::compileDatabaseStatement([](auto& database) {
        return service::open_access::AccessService::publicHistoryQuery(
            database,
            std::pmr::new_delete_resource(),
            "00000000-0000-0000-0000-000000000001",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
            20,
            40,
            3
        );
    });
    const auto& sql = statement.sql;
    for (const auto* fragment : { "device_data", "jsonb_typeof", "to_jsonb", "jsonb_set", "iot_utc_timestamp", "deleted_at" }) {
        require(sql.find(fragment) != std::string::npos, "public device history SQL lost BOOL 0/1 canonicalization or /api output shape");
    }
    require(sql.find("LIMIT ") != std::string::npos && sql.find("OFFSET ") != std::string::npos, "public device history SQL lost pagination");
}

} // namespace

int main() {
    try {
        requirePublicHistoryQueryShape();
        std::cout << "access service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "access service test failed: " << error.what() << '\n';
        return 1;
    }
}
