#include <iostream>
#include <stdexcept>
#include <string>

#include "service/common/message.h"

namespace {

// Requests stay on the accepting worker. Replies/frames have independent
// operation/session channels rather than a worker-wide notification stream.
void verifyDirectedAddresses() {
    using service::message::rpc::Contract;
    if (Contract::requests("instance", 0) == Contract::requests("instance", 1) ||
        Contract::requests("instance", 0) == Contract::requests("other", 0) ||
        Contract::reply("request-a") == Contract::reply("request-b") ||
        service::message::terminal::terminalEventTopic("node", "session-a") ==
            service::message::terminal::terminalEventTopic("node", "session-b") ||
        service::message::terminal::terminalEventTopic("node", "session-a") ==
            service::message::terminal::terminalAckTopic("node", "session-a")) {
        throw std::runtime_error("directed requests or operation channels overlap");
    }
}
} // namespace

int main() {
    try {
        verifyDirectedAddresses();
        std::cout << "worker isolation tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "worker isolation test failed: " << error.what() << '\n';
        return 1;
    }
}
