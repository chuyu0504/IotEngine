#pragma once

#include <memory_resource>
#include <string_view>
#include <ruvia/web/db/db_repository.h>

namespace service::common::database {

template <typename Entity> ruvia::db_predicate activeId(std::string_view id) {
    return Entity::template column<"id">() == id &&
           Entity::template column<"deleted_at">().is_null();
}

inline ruvia::db_expression emptyText(ruvia::db_query &query, std::string_view column,
                                     std::string_view table = {}) {
    return query.coalesce(
        {query.cast(query.column(column, table), ruvia::db_data_type::text), query.value("")});
}

inline ruvia::db_expression nullableUuid(ruvia::db_query &query, std::string_view value) {
    return query.cast(query.null_if(query.value(value), query.value("")), ruvia::db_data_type::uuid);
}

} // namespace service::common::database
