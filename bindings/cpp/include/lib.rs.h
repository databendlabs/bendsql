#pragma once
#include <cxx.h>

namespace bendsql {
    struct DatabendClientWrapper;
    struct DatabendConnectionWrapper;

    std::unique_ptr<DatabendClientWrapper> new_client(const std::string& dsn);
    std::unique_ptr<DatabendConnectionWrapper> get_connection(const DatabendClientWrapper& client);
    bool execute_query(const DatabendConnectionWrapper& connection, const std::string& query);
    std::string get_version(const DatabendClientWrapper& client);
    std::string query_row(const DatabendConnectionWrapper& connection, const std::string& query);
}
