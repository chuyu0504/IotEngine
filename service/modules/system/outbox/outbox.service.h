#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include <ruvia/web/db/db.h>
#include <ruvia/web/db/db_query.h>

#include "service/modules/system/outbox/outbox.entity.h"

namespace service::system {

class OutboxService final {
  public:
    template <typename Database>
    static ruvia::task<void> enqueueQueryChange(Database& database, std::string_view topic,
        std::string_view aggregateId, std::string_view eventId) {
        using entities::OutboxEventEntity;
        ruvia::db_query query;
        query.insert_into(OutboxEventEntity::table_name(), { OutboxEventEntity::column<"id">().name(), OutboxEventEntity::column<"event_type">().name(), OutboxEventEntity::column<"aggregate_type">().name(), OutboxEventEntity::column<"aggregate_id">().name(), OutboxEventEntity::column<"action">().name(), OutboxEventEntity::column<"schema_version">().name() })
            .values({query.cast(query.value(eventId), ruvia::db_data_type::uuid), query.value("query.changed"),
                query.value(topic), query.value(aggregateId), query.value("UPDATE"), query.value(std::int32_t{1})});
        (void)co_await database.execute(query);
    }

    template <typename Database>
    static ruvia::task<void> enqueueConfigEvent(Database& database, std::string_view aggregate, std::string_view action, std::string_view aggregateId, std::string_view eventId) {
        if (aggregate != "link" && aggregate != "device" && aggregate != "protocol" &&
            aggregate != "access_key" && aggregate != "webhook" && aggregate != "mqtt") {
            throw std::invalid_argument("unsupported outbox aggregate: " + std::string(aggregate));
        }
        using entities::OutboxEventEntity;
        ruvia::db_query query;
        query.insert_into(OutboxEventEntity::table_name(), { OutboxEventEntity::column<"id">().name(), OutboxEventEntity::column<"event_type">().name(), OutboxEventEntity::column<"aggregate_type">().name(), OutboxEventEntity::column<"aggregate_id">().name(), OutboxEventEntity::column<"action">().name(), OutboxEventEntity::column<"schema_version">().name(), OutboxEventEntity::column<"payload">().name() })
            .values({ query.cast(query.value(eventId), ruvia::db_data_type::uuid), query.value("config.changed"), query.value(aggregate), query.value(aggregateId), query.value(action), query.value(std::int32_t{ 1 }), query.cast(query.value("{}"), ruvia::db_data_type::jsonb) });
        (void)co_await database.execute(query);
    }
};

} // namespace service::system
