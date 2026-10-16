#pragma once

#include <cstdint>
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

inline ruvia::db_expression text(ruvia::db_query &query, ruvia::db_expression value) {
    return query.cast(value, ruvia::db_data_type::text);
}

inline ruvia::db_expression text(ruvia::db_query &query, std::string_view value) {
    return text(query, query.value(value));
}

inline ruvia::db_expression boolean(ruvia::db_query &query, bool value) {
    return query.cast(query.value(value), ruvia::db_data_type::boolean);
}

inline ruvia::db_expression integer(ruvia::db_query &query, std::int64_t value) {
    return query.cast(query.value(value), ruvia::db_data_type::integer);
}

inline ruvia::db_expression uuid(ruvia::db_query &query, std::string_view value) {
    return query.cast(query.value(value), ruvia::db_data_type::uuid);
}

// object -> 'key'
inline ruvia::db_expression jsonValue(ruvia::db_query &query, ruvia::db_expression object,
                                      std::string_view key) {
    return query.binary(object, ruvia::db_binary_operator::json_get, text(query, key));
}

// object ->> 'key'
inline ruvia::db_expression jsonText(ruvia::db_query &query, ruvia::db_expression object,
                                     std::string_view key) {
    return query.binary(object, ruvia::db_binary_operator::json_get_text, text(query, key));
}

inline ruvia::db_expression andAll(ruvia::db_query &query, ruvia::db_expression first,
                                   ruvia::db_expression second) {
    return query.binary(first, ruvia::db_binary_operator::and_value, second);
}

template <typename... Expressions>
ruvia::db_expression andAll(ruvia::db_query &query, ruvia::db_expression first,
                            ruvia::db_expression second, Expressions... rest) {
    return query.binary(first, ruvia::db_binary_operator::and_value, andAll(query, second, rest...));
}

} // namespace service::common::database
