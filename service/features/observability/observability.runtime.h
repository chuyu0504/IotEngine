#pragma once

#include <functional>
#include <stdexcept>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <ruvia/web/web_worker.h>

#include "service/features/observability/observability.service.h"

namespace service::observability {

struct Component {
    std::string name;
    std::vector<std::string> dependencies;
    std::function<void()> start;
    std::function<void()> stop;
};

class ComponentLifecycle final {
  public:
    explicit ComponentLifecycle(observability::RuntimeDiagnostics& observability)
        : observability_(observability) {}

    void add(Component component) {
        if (component.name.empty() || !component.start || !component.stop)
            throw std::invalid_argument("application component is incomplete");
        if (!names_.insert(component.name).second)
            throw std::invalid_argument("duplicate application component: " + component.name);
        components_.push_back(std::move(component));
    }

    template <typename Runtime>
    void addRuntime(std::string name, std::shared_ptr<Runtime> runtime,
                    ruvia::web_worker_handle worker) {
        add(Component{
            .name = std::move(name),
            .start = [runtime, worker] { runtime->start(worker); },
            .stop = [runtime] { runtime->stop(); }
        });
    }

    void start() {
        if (!started_.empty())
            return;
        std::unordered_set<std::string> ready;
        try {
            while (started_.size() != components_.size()) {
                bool progressed = false;
                for (auto& component : components_) {
                    if (ready.contains(component.name))
                        continue;
                    bool dependenciesReady = true;
                    for (const auto& dependency : component.dependencies) {
                        if (!names_.contains(dependency))
                            throw std::runtime_error("unknown dependency " + dependency +
                                                     " for component " + component.name);
                        dependenciesReady = dependenciesReady && ready.contains(dependency);
                    }
                    if (!dependenciesReady)
                        continue;
                    observability_.setComponentStatus(component.name,
                                             observability::ComponentState::Starting);
                    component.start();
                    observability_.setComponentStatus(component.name,
                                             observability::ComponentState::Ready);
                    ready.insert(component.name);
                    started_.push_back(&component);
                    progressed = true;
                }
                if (!progressed)
                    throw std::runtime_error("application component dependency cycle");
            }
        } catch (const std::exception& error) {
            if (started_.size() < components_.size())
                for (auto& component : components_)
                    if (!ready.contains(component.name))
                        observability_.setComponentStatus(component.name,
                                                 observability::ComponentState::Failed,
                                                 error.what());
            stop();
            throw;
        }
    }

    void stop() noexcept {
        while (!started_.empty()) {
            auto* component = started_.back();
            started_.pop_back();
            try {
                component->stop();
                observability_.setComponentStatus(component->name,
                                         observability::ComponentState::Stopped);
            } catch (const std::exception& error) {
                observability_.setComponentStatus(component->name,
                                         observability::ComponentState::Failed, error.what());
            } catch (...) {
                observability_.setComponentStatus(component->name,
                                         observability::ComponentState::Failed,
                                         "unknown stop failure");
            }
        }
    }

  private:
    observability::RuntimeDiagnostics& observability_;
    std::vector<Component> components_;
    std::unordered_set<std::string> names_;
    std::vector<Component*> started_;
};

} // namespace service::observability
