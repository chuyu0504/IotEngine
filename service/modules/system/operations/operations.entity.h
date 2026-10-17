#pragma once
#include <ruvia/web/db/db_entity.h>
#include <ruvia/web/redis/redis_entity.h>

#include "service/common/message.h"

namespace service::system::entities {
RUVIA_REDIS_ENTITY(WorkerSnapshotEntity, service::message::worker_metrics::kSnapshotTable, RUVIA_REDIS_COLUMN(id, ruvia::string, ruvia::redis_column_options{ .primary_key_ = true }), RUVIA_REDIS_COLUMN(metrics, ruvia::string), RUVIA_REDIS_COLUMN(ready, bool), RUVIA_REDIS_COLUMN(health, ruvia::string), RUVIA_REDIS_COLUMN(outbox_oldest_at_ms, std::int64_t));
} // namespace service::system::entities
