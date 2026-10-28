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

#include <ruvia/web/model.h>

namespace service::utils {

// 返回 index 处合法 UTF-8 序列（RFC 3629，拒绝过长编码与代理区）的字节数；非法时返回 0。
inline std::size_t utf8SequenceLength(std::string_view value, std::size_t index) noexcept {
    const auto at = [value](std::size_t position) {
        return static_cast<unsigned char>(value[position]);
    };
    const auto continuation = [&](std::size_t position) {
        return position < value.size() && (at(position) & 0xC0U) == 0x80U;
    };
    const auto between = [&](std::size_t position, unsigned char low, unsigned char high) {
        return position < value.size() && at(position) >= low && at(position) <= high;
    };
    const auto byte = at(index);
    if (byte < 0x80) {
        return 1;
    }
    if (byte >= 0xC2 && byte <= 0xDF) {
        return continuation(index + 1) ? 2 : 0;
    }
    if (byte == 0xE0) {
        return between(index + 1, 0xA0, 0xBF) && continuation(index + 2) ? 3 : 0;
    }
    if ((byte >= 0xE1 && byte <= 0xEC) || byte == 0xEE || byte == 0xEF) {
        return continuation(index + 1) && continuation(index + 2) ? 3 : 0;
    }
    if (byte == 0xED) {
        return between(index + 1, 0x80, 0x9F) && continuation(index + 2) ? 3 : 0;
    }
    if (byte == 0xF0) {
        return between(index + 1, 0x90, 0xBF) && continuation(index + 2) && continuation(index + 3) ? 4 : 0;
    }
    if (byte >= 0xF1 && byte <= 0xF3) {
        return continuation(index + 1) && continuation(index + 2) && continuation(index + 3) ? 4 : 0;
    }
    if (byte == 0xF4) {
        return between(index + 1, 0x80, 0x8F) && continuation(index + 2) && continuation(index + 3) ? 4 : 0;
    }
    return 0;
}

inline void appendJsonUnicodeEscape(std::string& output, unsigned char byte) {
    static constexpr std::array<char, 16> hex{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    output += "\\u00";
    output.push_back(hex[byte >> 4U]);
    output.push_back(hex[byte & 0x0FU]);
}

// 按 JSON 规范转义字符串内容：引号、反斜杠与 \b \f \n \r \t 用短转义，其余控制字符用 \u00xx；
// 非法 UTF-8 字节按其码位 U+00xx 转义，保证输出总是合法 JSON 文本。
inline void appendJsonEscaped(std::string& output, std::string_view value) {
    for (std::size_t index = 0; index < value.size();) {
        const auto ch = static_cast<unsigned char>(value[index]);
        if (ch >= 0x80) {
            if (const auto length = utf8SequenceLength(value, index); length != 0) {
                output.append(value.substr(index, length));
                index += length;
            } else {
                appendJsonUnicodeEscape(output, ch);
                ++index;
            }
            continue;
        }
        ++index;
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
                    appendJsonUnicodeEscape(output, ch);
                } else {
                    output.push_back(static_cast<char>(ch));
                }
        }
    }
}

inline std::string jsonEscape(std::string_view value) {
    std::string output;
    output.reserve(value.size() + 8);
    appendJsonEscaped(output, value);
    return output;
}

inline std::string jsonQuoted(std::string_view value) {
    std::string output;
    output.reserve(value.size() + 10);
    output.push_back('"');
    appendJsonEscaped(output, value);
    output.push_back('"');
    return output;
}

// 修复已编码 JSON 文本中的非法 UTF-8：合法序列原样保留，非法字节替换为 \u00xx，其他内容不变。
inline std::string sanitizeJsonUtf8(std::string_view json) {
    std::string output;
    output.reserve(json.size());
    for (std::size_t index = 0; index < json.size();) {
        if (const auto length = utf8SequenceLength(json, index); length != 0) {
            output.append(json.substr(index, length));
            index += length;
        } else {
            appendJsonUnicodeEscape(output, static_cast<unsigned char>(json[index]));
            ++index;
        }
    }
    return output;
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
    std::optional<ruvia::json_value> current{ ruvia::json_value::parse(root.view()) };
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

    JsonValueTemplate(std::string_view input, const Tokens& allowed, const Tokens& sparse = {}, bool unique = false, std::size_t maxBytes = 16384, unsigned maxDepth = 32, const Tokens& prefixes = {}) : capacity_(input.size()) {
        if (input.empty() || input.size() > maxBytes) {
            throw std::invalid_argument("JSON 模板字节长度无效");
        }
        const auto root = ruvia::json_value::parse(input);
        if (!root.is_object()) {
            throw std::invalid_argument("JSON 模板根节点必须是对象");
        }
        root_ = parse(root, allowed, sparse, unique, 0, maxDepth, prefixes);
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
        enum class Kind { Scalar,
                          Object,
                          Array,
                          Token } kind = Kind::Scalar;
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
        if (depth > maxDepth) {
            throw std::invalid_argument("JSON 模板嵌套层数过多");
        }
        Node node;
        if (raw.is_object()) {
            node.kind = Node::Kind::Object;
            Tokens keys;
            (void)raw.for_each_field([&](std::string_view key, const ruvia::json_value& child) {
                if (!keys.emplace(key).second) {
                    throw std::invalid_argument("JSON 模板对象键重复");
                }
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
            const bool named = std::any_of(prefixes.begin(), prefixes.end(), [&](const auto& prefix) {
                return token.starts_with(prefix) && token.size() > prefix.size();
            });
            if (!allowed.contains(token) && !named) {
                throw std::invalid_argument("JSON 模板包含未知占位符: " + std::string(token));
            }
            if (!references_.emplace(token).second && unique) {
                throw std::invalid_argument("JSON 模板占位符重复");
            }
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
            case Node::Kind::Scalar:
                result += node.value;
                return;
            case Node::Kind::Token: {
                const auto value = values.find(node.value);
                if (value == values.end()) {
                    throw std::invalid_argument("JSON 模板占位符缺少值: " + node.value);
                }
                result += value->second;
                return;
            }
            case Node::Kind::Object: {
                result += '{';
                bool first = true;
                for (const auto& [key, child] : node.members) {
                    if (child.kind == Node::Kind::Token && child.sparse && !values.contains(child.value)) {
                        continue;
                    }
                    if (!first) {
                        result += ',';
                    }
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
                    if (!first) {
                        result += ',';
                    }
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
