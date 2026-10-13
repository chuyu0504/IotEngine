#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <ruvia/web/model_object.h>

namespace service::utils {

inline std::string jsonEscape(std::string_view value) {
    static constexpr std::array<char, 16> hex{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    std::string output;
    output.reserve(value.size() + 8);
    for (const auto byte : value) {
        const auto ch = static_cast<unsigned char>(byte);
        switch (ch) {
            case '"':
                output += "\\\"";
                break;
            case '\\':
                output += "\\\\";
                break;
            case '\b':
                output += "\\b";
                break;
            case '\f':
                output += "\\f";
                break;
            case '\n':
                output += "\\n";
                break;
            case '\r':
                output += "\\r";
                break;
            case '\t':
                output += "\\t";
                break;
            default:
                if (ch < 0x20) {
                    output += "\\u00";
                    output.push_back(hex[ch >> 4U]);
                    output.push_back(hex[ch & 0x0FU]);
                } else {
                    output.push_back(static_cast<char>(ch));
                }
        }
    }
    return output;
}

inline std::string jsonQuoted(std::string_view value) {
    return "\"" + jsonEscape(value) + "\"";
}

// 裸字段名表示顶层键；以 / 开始时使用 RFC 6901 JSON Pointer。
inline std::vector<std::string> jsonFieldSegments(std::string_view path) {
    if (path.empty()) {
        return {};
    }
    if (path.front() != '/') {
        return { std::string(path) };
    }
    std::vector<std::string> result;
    path.remove_prefix(1);
    for (;;) {
        const auto end = path.find('/');
        const auto part = path.substr(0, end);
        std::string key;
        for (std::size_t i = 0; i < part.size(); ++i) {
            if (part[i] != '~') {
                key += part[i];
                continue;
            }
            if (++i == part.size() || (part[i] != '0' && part[i] != '1')) {
                throw std::invalid_argument("invalid JSON Pointer escape");
            }
            key += part[i] == '0' ? '~' : '/';
        }
        result.push_back(std::move(key));
        if (result.size() > 32) {
            throw std::invalid_argument("JSON Pointer exceeds 32 levels");
        }
        if (end == std::string_view::npos) {
            return result;
        }
        path.remove_prefix(end + 1);
    }
}

inline std::optional<ruvia::json_value> jsonField(const ruvia::json_value& root, std::string_view path) {
    auto current = ruvia::json_value::parse(root.view());
    if (!current) {
        return std::nullopt;
    }
    for (const auto& key : jsonFieldSegments(path)) {
        if (current->is_object()) {
            current = current->get<ruvia::json_value>(key);
        } else if (current->is_array()) {
            std::size_t index = 0;
            const auto number = std::from_chars(key.data(), key.data() + key.size(), index);
            if (key.empty() || (key.size() > 1 && key.front() == '0') || number.ec != std::errc{} || number.ptr != key.data() + key.size()) {
                return std::nullopt;
            }
            std::optional<ruvia::json_value> selected;
            std::size_t position = 0;
            (void)current->for_each_element([&](const ruvia::json_value& item) {
                if (position++ == index) {
                    selected = ruvia::json_value::parse(item.view());
                    return false;
                }
                return true;
            });
            current = std::move(selected);
        } else {
            return std::nullopt;
        }
        if (!current) {
            return std::nullopt;
        }
    }
    return current;
}


// Compile JSON value placeholders without interpreting their domain-specific meaning.
class JsonValueTemplate final {
  public:
    using Tokens = std::set<std::string, std::less<>>;
    using Values = std::map<std::string, std::string, std::less<>>;

    JsonValueTemplate(std::string_view input, const Tokens& allowed, const Tokens& sparse = {}, bool unique = false,
                      std::size_t maxBytes = 16384, unsigned maxDepth = 32, const Tokens& prefixes = {}) : capacity_(input.size()) {
        if (input.empty() || input.size() > maxBytes) throw std::invalid_argument("JSON 模板字节长度无效");
        const auto root = ruvia::json_value::parse(input);
        if (!root || !root->is_object()) throw std::invalid_argument("JSON 模板根节点必须是对象");
        root_ = parse(*root, allowed, sparse, unique, 0, maxDepth, prefixes);
    }

    bool references(std::string_view token) const { return references_.contains(token); }
    const Tokens& references() const noexcept { return references_; }

    std::string render(const Values& values) const {
        std::string result;
        result.reserve(capacity_);
        append(root_, values, result);
        return result;
    }

  private:
    struct ObjectMember;

    struct Node {
        enum class Kind { Scalar, Object, Array, Token } kind = Kind::Scalar;
        std::string value;
        bool sparse = false;
        std::vector<ObjectMember> members;
        std::vector<Node> elements;
    };

    struct ObjectMember {
        std::string key;
        Node value;
    };

    Node parse(const ruvia::json_value& raw, const Tokens& allowed, const Tokens& sparse, bool unique, unsigned depth, unsigned maxDepth, const Tokens& prefixes) {
        if (depth > maxDepth) throw std::invalid_argument("JSON 模板嵌套层数过多");
        Node node;
        if (raw.is_object()) {
            node.kind = Node::Kind::Object;
            Tokens keys;
            (void)raw.for_each_field([&](std::string_view key, const ruvia::json_value& child) {
                if (!keys.emplace(key).second) throw std::invalid_argument("JSON 模板对象键重复");
                node.members.emplace_back(jsonQuoted(key), parse(child, allowed, sparse, unique, depth + 1, maxDepth, prefixes));
                return true;
            });
        } else if (raw.is_array()) {
            node.kind = Node::Kind::Array;
            (void)raw.for_each_element([&](const ruvia::json_value& child) {
                node.elements.push_back(parse(child, allowed, sparse, unique, depth + 1, maxDepth, prefixes));
                return true;
            });
        } else if (const auto text = raw.get<ruvia::string>(); text && text->view().starts_with('$')) {
            const auto token = text->view();
            const bool named = std::any_of(prefixes.begin(), prefixes.end(), [&](const auto& prefix) { return token.starts_with(prefix) && token.size() > prefix.size(); });
            if (!allowed.contains(token) && !named) throw std::invalid_argument("JSON 模板包含未知占位符: " + std::string(token));
            if (!references_.emplace(token).second && unique) throw std::invalid_argument("JSON 模板占位符重复");
            node.kind = Node::Kind::Token;
            node.value = token;
            node.sparse = sparse.contains(token);
        } else {
            node.value = raw.view();
        }
        return node;
    }

    static void append(const Node& node, const Values& values, std::string& result) {
        switch (node.kind) {
            case Node::Kind::Scalar: result += node.value; return;
            case Node::Kind::Token: {
                const auto value = values.find(node.value);
                if (value == values.end()) throw std::invalid_argument("JSON 模板占位符缺少值: " + node.value);
                result += value->second;
                return;
            }
            case Node::Kind::Object: {
                result += '{';
                bool first = true;
                for (const auto& [key, child] : node.members) {
                    if (child.kind == Node::Kind::Token && child.sparse && !values.contains(child.value)) continue;
                    if (!first) result += ',';
                    first = false;
                    result += key;
                    result += ':';
                    append(child, values, result);
                }
                result += '}';
                return;
            }
            case Node::Kind::Array: {
                result += '[';
                bool first = true;
                for (const auto& child : node.elements) {
                    if (!first) result += ',';
                    first = false;
                    append(child, values, result);
                }
                result += ']';
                return;
            }
        }
        throw std::invalid_argument("JSON 模板节点无效");
    }

    Node root_;
    Tokens references_;
    std::size_t capacity_;
};

} // namespace service::utils
