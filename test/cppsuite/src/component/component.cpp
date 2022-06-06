/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "component.h"

#include <thread>

#include "src/common/constants.h"
#include "src/common/logger.h"

namespace test_harness {
Component::Component(const std::string &name, Configuration *config) : _config(config), _name(name)
{
}

Component::~Component()
{
    delete _config;
}

void
Component::Load()
{
    Logger::LogMessage(LOG_INFO, "Loading component: " + _name);
    _enabled = _config->GetOptionalBool(kEnabledConfig, true);
    /* If we're not enabled we shouldn't be running. */
    _running = _enabled;

    if (!_enabled)
        return;

    _sleepTimeMs = _config->GetThrottleMs();
}

void
Component::Run()
{
    Logger::LogMessage(LOG_INFO, "Running component: " + _name);
    while (_enabled && _running) {
        DoWork();
        std::this_thread::sleep_for(std::chrono::milliseconds(_sleepTimeMs));
    }
}

void
Component::DoWork()
{
    /* Not implemented. */
}

bool
Component::IsEnabled() const
{
    return (_enabled);
}

void
Component::EndRun()
{
    _running = false;
}

void
Component::Finish()
{
    Logger::LogMessage(LOG_INFO, "Running finish stage of component: " + _name);
}
} // namespace test_harness
