#include <cstdint>
#include <iostream>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "service/features/edge/gateway/gateway.entity.h"
#include "service/features/edge/gateway/gateway.service.h"
#include "service/modules/edge_node/edge_node.service.h"

int main() {
    try {
        const auto approved = service::edge::gateway::EnrollmentRecord::decode("node-1|approved");
        if (!approved || approved->nodeId != "node-1" || approved->status != "approved") {
            throw std::runtime_error("gateway enrollment record lost node or approval status");
        }
        if (service::edge::gateway::EnrollmentRecord::decode("node-1").has_value()) {
            throw std::runtime_error("gateway accepted an incomplete enrollment record");
        }
        const auto pending = service::edge::gateway::EnrollmentRecord::decode("node-1|pending");
        if (!pending || pending->status != "pending") {
            throw std::runtime_error("gateway enrollment record changed pending status");
        }
        service::edge::pb::CommandRequest capableCommand;
        capableCommand.set_timeout_ms(5000);
        constexpr std::int64_t persistedDeadlineMs = 1780000000123;
        service::edge::gateway::GatewayService::applyCommandDeadline(
            capableCommand, true, persistedDeadlineMs);
        if (!capableCommand.has_start_before_ms() ||
            capableCommand.start_before_ms() != persistedDeadlineMs ||
            capableCommand.timeout_ms() != 5000) {
            throw std::runtime_error("negotiated command deadline did not preserve the persisted millisecond value");
        }
        service::edge::pb::CommandRequest legacyCommand;
        legacyCommand.set_timeout_ms(5000);
        service::edge::gateway::GatewayService::applyCommandDeadline(
            legacyCommand, false, persistedDeadlineMs);
        if (legacyCommand.has_start_before_ms() || legacyCommand.timeout_ms() != 5000) {
            throw std::runtime_error("legacy command wire behavior changed");
        }
        bool rejectedUnnegotiatedField = false;
        legacyCommand.set_start_before_ms(persistedDeadlineMs);
        try {
            service::edge::gateway::GatewayService::applyCommandDeadline(
                legacyCommand, false, persistedDeadlineMs);
        } catch (const std::invalid_argument&) {
            rejectedUnnegotiatedField = true;
        }
        if (!rejectedUnnegotiatedField) {
            throw std::runtime_error("legacy session accepted an unnegotiated command deadline field");
        }
        bool rejectedMissingDeadline = false;
        try {
            service::edge::pb::CommandRequest invalidCommand;
            service::edge::gateway::GatewayService::applyCommandDeadline(
                invalidCommand, true, std::nullopt);
        } catch (const std::invalid_argument&) {
            rejectedMissingDeadline = true;
        }
        if (!rejectedMissingDeadline) {
            throw std::runtime_error("negotiated command deadline silently proceeded without its database value");
        }
        {
            constexpr std::string_view nodeId = "00000000-0000-4000-8000-0000000000aa";
            const auto removal = service::edge_node::EdgeService::pendingEnrollmentRemoval(std::pmr::new_delete_resource(), nodeId);
            const auto statement = removal.compile(ruvia::db_driver::postgresql, std::pmr::new_delete_resource(), ruvia::db_parameter_mode::literal);
            const std::string sql(statement.sql());
            const auto where = sql.find("WHERE");
            if (sql.find("DELETE FROM") == std::string::npos || sql.find("edge_node") == std::string::npos) {
                throw std::runtime_error("pending enrollment removal is not a DELETE on edge_node");
            }
            if (where == std::string::npos || sql.find("enrollment_status", where) == std::string::npos ||
                sql.find("'pending'", where) == std::string::npos || sql.find(nodeId, where) == std::string::npos ||
                sql.find(" AND ", where) == std::string::npos) {
                throw std::runtime_error("pending enrollment removal does not restrict deletion to the pending node");
            }
            if (sql.find("RETURNING") == std::string::npos) {
                throw std::runtime_error("pending enrollment removal cannot detect a concurrent status change");
            }
        }
        std::cout << "edge service tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "edge service test failed: " << error.what() << '\n';
        return 1;
    }
}
