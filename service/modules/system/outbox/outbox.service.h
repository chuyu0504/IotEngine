#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_repository.h>

#include "service/modules/system/outbox/outbox.entity.h"

namespace service::system {

class OutboxService final {
  public:
    template <typename Database>
    static ruvia::task<void> enqueueQueryChange(Database& database, std::string_view topic, std::string_view aggregateId, std::string_view eventId) {
        using entities::OutboxEventEntity;
        OutboxEventEntity event;
        event.set<"id">(eventId);
        event.set<"event_type">("query.changed");
        event.set<"aggregate_type">(topic);
        event.set<"aggregate_id">(aggregateId);
        event.set<"action">("UPDATE");
        event.set<"schema_version">(1);
        (void)co_await database.template repository<OutboxEventEntity>().insert(event);
    }

    template <typename Database>
    static ruvia::task<void> enqueueConfigEvent(Database& database, std::string_view aggregate, std::string_view action, std::string_view aggregateId, std::string_view eventId) {
        if (aggregate != "link" && aggregate != "device" && aggregate != "protocol" &&
            aggregate != "access_key" && aggregate != "webhook" && aggregate != "mqtt") {
            throw std::invalid_argument("unsupported outbox aggregate: " + std::string(aggregate));
        }
        using entities::OutboxEventEntity;
        OutboxEventEntity event;
        event.set<"id">(eventId);
        event.set<"event_type">("config.changed");
        event.set<"aggregate_type">(aggregate);
        event.set<"aggregate_id">(aggregateId);
        event.set<"action">(action);
        event.set<"schema_version">(1);
        event.set<"payload">("{}");
        (void)co_await database.template repository<OutboxEventEntity>().insert(event);
    }
};

} // namespace service::system
