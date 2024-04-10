/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-04-03
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#include "internal/resource.hh"

#include <vector>
#include <map>
#include <sstream>
#include <charconv>

#include <maxbase/checksum.hh>
#include <maxbase/jansson.hh>
#include <maxbase/string.hh>
#include <maxscale/cachingparser.hh>
#include <maxscale/cn_strings.hh>
#include <maxscale/http.hh>
#include <maxscale/json_api.hh>
#include <maxscale/listener.hh>
#include <maxscale/mainworker.hh>
#include <maxscale/modulecmd.hh>
#include <maxscale/protocol/mariadb/mariadbparser.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/routingworker.hh>

#include "internal/admin.hh"
#include "internal/adminusers.hh"
#include "internal/config.hh"
#include "internal/config_runtime.hh"
#include "internal/configmanager.hh"
#include "internal/filter.hh"
#include "internal/http_sql.hh"
#include "internal/httprequest.hh"
#include "internal/httpresponse.hh"
#include "internal/modules.hh"
#include "internal/monitormanager.hh"
#include "internal/servermanager.hh"
#include "internal/service.hh"
#include "internal/session.hh"
#include "internal/profiler.hh"

using std::map;
using std::string;
using std::stringstream;
using maxscale::Listener;
using maxscale::Monitor;

using namespace std::literals::string_literals;

namespace
{
const char CN_FORCE[] = "force";
const char CN_YES[] = "yes";

enum class ObjectType
{
    SERVICE,
    SERVER,
    MONITOR,
    FILTER,
    LISTENER,
};

// Helper for extracting a specific relationship
HttpResponse get_relationship(const HttpRequest& request, ObjectType type, const std::string& relationship)
{
    json_t* json = nullptr;
    auto name = request.uri_part(1);

    switch (type)
    {
    case ObjectType::SERVICE:
        json = service_to_json(Service::find(name), request.host());
        break;

    case ObjectType::SERVER:
        json = ServerManager::server_to_json_resource(
            ServerManager::find_by_unique_name(name), request.host());
        break;

    case ObjectType::MONITOR:
        json = MonitorManager::monitor_to_json(MonitorManager::find_monitor(name.c_str()), request.host());
        break;

    case ObjectType::FILTER:
        json = filter_find(name)->to_json(request.host());
        break;

    case ObjectType::LISTENER:
        json = Listener::find(name)->to_json_resource(request.host());
        break;

    default:
        mxb_assert(!true);
        return HttpResponse(MHD_HTTP_INTERNAL_SERVER_ERROR);
    }

    std::string final_path = MXS_JSON_PTR_RELATIONSHIPS + "/"s + relationship;
    auto rel = json_incref(mxb::json_ptr(json, final_path.c_str()));
    json_decref(json);

    return HttpResponse(rel ? MHD_HTTP_OK : MHD_HTTP_NOT_FOUND, rel);
}

uint64_t to_session_id(std::string_view str)
{
    uint64_t id = 0;
    return std::from_chars(str.begin(), str.end(), id).ptr == str.end() ? id : 0;
}
}

static bool log_redirect(int level, std::string_view msg)
{
    if (level < LOG_WARNING)    // Lower is more severe
    {
        config_runtime_add_error(msg);
        return true;
    }

    return false;
}

bool Resource::match(const HttpRequest& request) const
{
    bool rval = false;

    if (request.uri_part_count() == m_path.size() || m_is_glob)
    {
        rval = true;
        size_t parts = std::min(request.uri_part_count(), m_path.size());

        for (size_t i = 0; i < parts; i++)
        {
            if (m_path[i] != request.uri_part(i)
                && !matching_variable_path(m_path[i], request.uri_part(i)))
            {
                rval = false;
                break;
            }
        }
    }

    return rval;
}

bool Resource::part_matches(const std::string& part, size_t depth) const
{
    bool rval = false;

    if (m_path.size() > depth)
    {
        rval = m_path[depth] == part || matching_variable_path(m_path[depth], part);
    }

    return rval;
}

bool Resource::variable_part_mismatch(const std::deque<std::string>& path) const
{
    bool rval = m_path.size() == path.size();

    if (rval)
    {
        for (size_t i = 0; i < m_path.size(); i++)
        {
            if (m_path[i] != path[i] && !is_variable_part(i))
            {
                rval = false;
                break;
            }
        }
    }

    return rval;
}

static void remove_null_parameters(json_t* json)
{
    if (json_t* parameters = mxb::json_ptr(json, MXS_JSON_PTR_PARAMETERS))
    {
        const char* key;
        json_t* value;
        void* tmp;

        json_object_foreach_safe(parameters, tmp, key, value)
        {
            if (json_is_null(value))
            {
                json_object_del(parameters, key);
            }
        }
    }
}

HttpResponse Resource::call(const HttpRequest& request) const
{
    return m_cb(request);
}

bool Resource::is_variable_part(size_t i) const
{
    return i < m_path.size() && (m_path[i][0] == ':' || m_path[i][0] == '?');
}

bool Resource::matching_variable_path(const string& path, const string& target) const
{
    bool rval = false;

    if (path[0] == ':')
    {
        if ((path == ":service" && Service::find(target))
            || (path == ":server" && ServerManager::find_by_unique_name(target))
            || (path == ":filter" && filter_find(target.c_str()))
            || (path == ":monitor" && MonitorManager::find_monitor(target.c_str()))
            || (path == ":module" && (target == mxs::Config::get().specification().module()
                                      || target == Server::specification().module()
                                      || is_mxs_module(target)))
            || (path == ":inetuser" && admin_inet_user_exists(target.c_str()) != mxs::USER_ACCOUNT_UNKNOWN)
            || (path == ":listener" && Listener::find(target.c_str()))
            || (path == ":connection_id" && HttpSql::is_connection(target))
            || (path == ":query_id" && HttpSql::is_query(target)))
        {
            rval = true;
        }
        else if (path == ":session")
        {
            // At this point the only thing that has to be checked is that the argument looks like a valid
            // session ID. The actual lookup for the ID is done later when the correct endpoint is found.
            rval = to_session_id(target) > 0;
        }
        else if (path == ":thread")
        {
            char* end;
            int index = strtol(target.c_str(), &end, 10);

            if (*end == '\0' && mxs::RoutingWorker::get_by_index(index))
            {
                rval = true;
            }
        }
    }
    else if (path == "?")
    {
        /** Wildcard match */
        rval = true;
    }

    return rval;
}

void Resource::add_constraint(resource_constraint type)
{
    m_constraints |= static_cast<uint32_t>(type);
}

bool Resource::requires_body() const
{
    return m_constraints & REQUIRE_BODY;
}

bool Resource::requires_sync() const
{
    return m_constraints & REQUIRE_SYNC;
}

namespace
{

bool option_rdns_is_on(const HttpRequest& request)
{
    return request.is_truthy_option("rdns");
}

static bool drop_path_part(std::string& path)
{
    size_t pos = path.find_last_of('/');
    bool rval = false;

    if (pos != std::string::npos)
    {
        path.erase(pos);
        rval = true;
    }

    return rval && path.length();
}

/**
 * Class that keeps track of resource modification times
 */
class ResourceWatcher
{
public:

    ResourceWatcher()
        : m_init(time(NULL))
    {
    }

    void modify(const std::string& orig_path)
    {
        std::string path = orig_path;

        do
        {
            m_last_modified[path] = time(NULL);
        }
        while (drop_path_part(path));
    }

    time_t last_modified(const string& path) const
    {
        map<string, time_t>::const_iterator it = m_last_modified.find(path);

        if (it != m_last_modified.end())
        {
            return it->second;
        }

        // Resource has not yet been updated
        return m_init;
    }

private:
    time_t              m_init;
    map<string, time_t> m_last_modified;
};

HttpResponse cb_stop_monitor(const HttpRequest& request)
{
    string mon_name = request.uri_part(1);
    Monitor* monitor = MonitorManager::find_monitor(mon_name.c_str());
    if (monitor)
    {
        auto [ok, errmsg] = MonitorManager::soft_stop_monitor(monitor);
        if (ok)
        {
            return HttpResponse(MHD_HTTP_NO_CONTENT);
        }
        else
        {
            json_t* error = mxs_json_error("Could not stop monitor '%s'. %s", mon_name.c_str(),
                                           errmsg.c_str());
            return HttpResponse(MHD_HTTP_BAD_REQUEST, error);
        }
    }
    else
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }
}

HttpResponse cb_start_monitor(const HttpRequest& request)
{
    Monitor* monitor = MonitorManager::find_monitor(request.uri_part(1).c_str());
    if (monitor)
    {
        MonitorManager::start_monitor(monitor);
    }
    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_stop_service(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    service->stop();

    if (request.is_truthy_option(CN_FORCE))
    {
        Session::kill_all(service);
    }

    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_start_service(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    service->start();
    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_stop_listener(const HttpRequest& request)
{
    auto listener = Listener::find(request.uri_part(1).c_str());
    listener->stop();

    if (request.is_truthy_option(CN_FORCE))
    {
        Session::kill_all(listener.get());
    }

    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_start_listener(const HttpRequest& request)
{
    auto listener = Listener::find(request.uri_part(1).c_str());
    listener->start();
    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_create_server(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_server_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_server(const HttpRequest& request)
{
    auto server = ServerManager::find_by_unique_name(request.uri_part(1));
    mxb_assert(server && request.get_json());

    if (runtime_alter_server_from_json(server, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse do_alter_server_relationship(const HttpRequest& request, const char* type)
{
    auto server = ServerManager::find_by_unique_name(request.uri_part(1));
    mxb_assert(server && request.get_json());

    if (runtime_alter_server_relationships_from_json(server, type, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_server_service_relationship(const HttpRequest& request)
{
    return do_alter_server_relationship(request, "services");
}

HttpResponse cb_alter_server_monitor_relationship(const HttpRequest& request)
{
    return do_alter_server_relationship(request, "monitors");
}

HttpResponse cb_create_monitor(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_monitor_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_create_filter(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_filter_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_create_service(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_service_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_create_service_listener(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service && request.get_json());

    if (runtime_create_listener_from_json(request.get_json(), service))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_create_listener(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_listener_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_monitor(const HttpRequest& request)
{
    Monitor* monitor = MonitorManager::find_monitor(request.uri_part(1).c_str());
    mxb_assert(monitor && request.get_json());

    if (runtime_alter_monitor_from_json(monitor, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_monitor_relationship(const HttpRequest& request, const char* type)
{
    Monitor* monitor = MonitorManager::find_monitor(request.uri_part(1).c_str());
    mxb_assert(monitor && request.get_json());

    if (runtime_alter_monitor_relationships_from_json(monitor, type, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_monitor_server_relationship(const HttpRequest& request)
{
    return cb_alter_monitor_relationship(request, CN_SERVERS);
}

HttpResponse cb_alter_monitor_service_relationship(const HttpRequest& request)
{
    return cb_alter_monitor_relationship(request, CN_SERVICES);
}

HttpResponse cb_alter_service(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service && request.get_json());

    if (runtime_alter_service_from_json(service, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_filter(const HttpRequest& request)
{
    auto filter = filter_find(request.uri_part(1).c_str());
    mxb_assert(filter && request.get_json());

    if (runtime_alter_filter_from_json(filter, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_listener(const HttpRequest& request)
{
    auto listener = Listener::find(request.uri_part(1).c_str());
    mxb_assert(listener && request.get_json());

    if (runtime_alter_listener_from_json(listener, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_service_relationship(const HttpRequest& request, const char* type)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service && request.get_json());

    if (runtime_alter_service_relationships_from_json(service, type, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_service_server_relationship(const HttpRequest& request)
{
    return cb_alter_service_relationship(request, CN_SERVERS);
}

HttpResponse cb_alter_service_service_relationship(const HttpRequest& request)
{
    return cb_alter_service_relationship(request, CN_SERVICES);
}

HttpResponse cb_alter_service_filter_relationship(const HttpRequest& request)
{
    return cb_alter_service_relationship(request, CN_FILTERS);
}

HttpResponse cb_alter_service_monitor_relationship(const HttpRequest& request)
{
    return cb_alter_service_relationship(request, CN_MONITORS);
}

HttpResponse cb_alter_session_filter_relationship(const HttpRequest& request)
{
    uint64_t id = to_session_id(request.uri_part(1));
    bool ok = false;

    // Fake the payload so that it looks like a normal PATCH request
    json_t* j = json_pack("{s: {s: {s: {s: O}}}}",
                          "data", "relationships", "filters", "data",
                          json_object_get(request.get_json(), "data"));

    bool found = mxs::RoutingWorker::execute_for_session(id, [&](MXS_SESSION* session){
        ok = static_cast<Session*>(session)->update(j);
    });

    json_decref(j);

    if (!found)
    {
        return HttpResponse(MHD_HTTP_NOT_FOUND);
    }

    // FIXME: The errors from Session::update() are not returned up to the MainWorker and are instead logged
    // FIXME: into the MaxScale log.
    return ok ? HttpResponse(MHD_HTTP_OK) : HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_qc(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (mxs::CachingParser::set_properties(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_delete_server(const HttpRequest& request)
{
    auto server = ServerManager::find_by_unique_name(request.uri_part(1).c_str());
    mxb_assert(server);

    if (runtime_destroy_server(server, request.is_truthy_option(CN_FORCE)))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_delete_monitor(const HttpRequest& request)
{
    Monitor* monitor = MonitorManager::find_monitor(request.uri_part(1).c_str());
    mxb_assert(monitor);

    if (runtime_destroy_monitor(monitor, request.is_truthy_option(CN_FORCE)))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_delete_service_listener(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service);
    auto listener = Listener::find(request.uri_part(3));
    mxb_assert(listener);

    if (listener->service() != service)
    {
        // Both the listener and the service exist but the listener doesn't point to the given service.
        return HttpResponse(MHD_HTTP_NOT_FOUND);
    }
    else if (!runtime_destroy_listener(listener))
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }

    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_delete_listener(const HttpRequest& request)
{
    auto listener = Listener::find(request.uri_part(1).c_str());
    mxb_assert(listener);

    if (!runtime_destroy_listener(listener))
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }

    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_delete_service(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service);

    if (runtime_destroy_service(service, request.is_truthy_option(CN_FORCE)))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_delete_filter(const HttpRequest& request)
{
    auto filter = filter_find(request.uri_part(1).c_str());
    mxb_assert(filter);

    if (runtime_destroy_filter(filter, request.is_truthy_option(CN_FORCE)))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}
HttpResponse cb_all_servers(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, ServerManager::server_list_to_json(request.host()));
}

HttpResponse cb_get_server(const HttpRequest& request)
{
    auto server = ServerManager::find_by_unique_name(request.uri_part(1));
    mxb_assert(server);
    return HttpResponse(MHD_HTTP_OK, ServerManager::server_to_json_resource(server, request.host()));
}

HttpResponse cb_all_services(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, service_list_to_json(request.host()));
}

HttpResponse cb_get_service(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service);
    return HttpResponse(MHD_HTTP_OK, service_to_json(service, request.host()));
}

HttpResponse cb_get_all_service_listeners(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    return HttpResponse(MHD_HTTP_OK, service_listener_list_to_json(service, request.host()));
}

HttpResponse cb_get_service_listener(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    std::string listener = request.uri_part(3);
    mxb_assert(service);

    if (service_has_named_listener(service, listener.c_str()))
    {
        return HttpResponse(MHD_HTTP_OK, service_listener_to_json(service, listener.c_str(), request.host()));
    }

    return HttpResponse(MHD_HTTP_NOT_FOUND);
}

HttpResponse cb_get_all_listeners(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, Listener::to_json_collection(request.host()));
}

HttpResponse cb_get_listener(const HttpRequest& request)
{
    auto listener = Listener::find(request.uri_part(1).c_str());
    return HttpResponse(MHD_HTTP_OK, listener->to_json_resource(request.host()));
}

HttpResponse cb_all_filters(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, FilterDef::filter_list_to_json(request.host()));
}

HttpResponse cb_get_filter(const HttpRequest& request)
{
    auto filter = filter_find(request.uri_part(1).c_str());
    mxb_assert(filter);
    return HttpResponse(MHD_HTTP_OK, filter->to_json(request.host()));
}

HttpResponse cb_all_monitors(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, MonitorManager::monitor_list_to_json(request.host()));
}

HttpResponse cb_get_monitor(const HttpRequest& request)
{
    Monitor* monitor = MonitorManager::find_monitor(request.uri_part(1).c_str());
    mxb_assert(monitor);
    return HttpResponse(MHD_HTTP_OK, MonitorManager::monitor_to_json(monitor, request.host()));
}

HttpResponse cb_all_sessions(const HttpRequest& request)
{
    bool rdns = option_rdns_is_on(request);
    return HttpResponse(MHD_HTTP_OK, session_list_to_json(request.host(), rdns));
}

HttpResponse cb_get_session(const HttpRequest& request)
{
    uint64_t id = to_session_id(request.uri_part(1));
    json_t* json = nullptr;

    bool found = mxs::RoutingWorker::execute_for_session(id, [&](MXS_SESSION* session){
        json = session_to_json(session, request.host(), option_rdns_is_on(request));
    });
    mxb_assert_message(!found || json, "A found session must produce JSON output");

    return found ? HttpResponse(MHD_HTTP_OK, json) : HttpResponse(MHD_HTTP_NOT_FOUND);
}

HttpResponse cb_get_server_service_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVER, "services");
}

HttpResponse cb_get_server_monitor_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVER, "monitors");
}

HttpResponse cb_get_monitor_server_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::MONITOR, "servers");
}

HttpResponse cb_get_monitor_service_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::MONITOR, "services");
}

HttpResponse cb_get_service_server_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVICE, "servers");
}

HttpResponse cb_get_service_service_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVICE, "services");
}

HttpResponse cb_get_service_filter_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVICE, "filters");
}

HttpResponse cb_get_service_monitor_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVICE, "monitors");
}

HttpResponse cb_get_service_listener_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::SERVICE, "listeners");
}

HttpResponse cb_get_filter_service_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::FILTER, "services");
}

HttpResponse cb_get_listener_service_relationship(const HttpRequest& request)
{
    return get_relationship(request, ObjectType::LISTENER, "services");
}

HttpResponse cb_maxscale(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, mxs::Config::get().maxscale_to_json(request.host()));
}

HttpResponse cb_alter_maxscale(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_alter_maxscale_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_logs(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, mxs_logs_to_json(request.host()));
}

template<auto func>
HttpResponse get_log_data_json(const HttpRequest& request)
{
    int rows = 50;
    auto size = request.get_option("page[size]");
    auto cursor = request.get_option("page[cursor]");
    auto priority = mxb::strtok(request.get_option("priority"), ",");

    if (!size.empty())
    {
        char* end;
        rows = strtol(size.c_str(), &end, 10);

        if (rows <= 0 || *end != '\0')
        {
            MXB_ERROR("Invalid value for 'page[size]': %s", size.c_str());
            return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
        }
    }

    return HttpResponse(MHD_HTTP_OK, func(request.host(), cursor, rows, {priority.begin(), priority.end()}));
}

HttpResponse cb_log_data(const HttpRequest& request)
{
    return get_log_data_json<mxs_log_data_to_json>(request);
}

HttpResponse cb_log_entries(const HttpRequest& request)
{
    return get_log_data_json<mxs_log_entries_to_json>(request);
}

HttpResponse cb_log_stream(const HttpRequest& request)
{
    auto cursor = request.get_option("page[cursor]");
    auto priority = mxb::strtok(request.get_option("priority"), ",");

    if (auto fn = mxs_logs_stream(cursor, {priority.begin(), priority.end()}))
    {
        return HttpResponse(fn);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_flush(const HttpRequest& request)
{
    int code = MHD_HTTP_INTERNAL_SERVER_ERROR;

    // Flush logs
    if (mxs_log_rotate())
    {
        code = MHD_HTTP_NO_CONTENT;
    }

    return HttpResponse(code);
}

HttpResponse cb_tls_reload(const HttpRequest& request)
{
    if (!ServerManager::reload_tls() || !Listener::reload_tls() || !mxs_admin_reload_tls())
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }

    MXB_NOTICE("TLS certificates successfully reloaded.");
    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_thread_rebalance(const HttpRequest& request)
{
    string thread = request.uri_part(2);
    mxb_assert(!thread.empty());    // Should have been checked already.

    long idx;
    MXB_AT_DEBUG(bool rv = ) mxb::get_long(thread, &idx);
    mxb_assert(rv);

    mxs::RoutingWorker* worker = mxs::RoutingWorker::get_by_index(idx);
    mxb_assert(worker);

    if (runtime_thread_rebalance(*worker,
                                 request.get_option("sessions"),
                                 request.get_option("recipient")))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_threads_rebalance(const HttpRequest& request)
{
    if (runtime_threads_rebalance(request.get_option("threshold")))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_reload_users(const HttpRequest& request)
{
    Service* service = Service::find(request.uri_part(1).c_str());
    mxb_assert(service);

    service->user_account_manager()->update_user_accounts();

    return HttpResponse(MHD_HTTP_NO_CONTENT);
}

HttpResponse cb_all_threads(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, mxs_rworker_list_to_json(request.host()));
}

HttpResponse cb_qc(const HttpRequest& request)
{
    json_t* json = mxs::CachingParser::get_properties_as_resource(request.host()).release();
    return HttpResponse(MHD_HTTP_OK, json);
}

HttpResponse cb_qc_classify(const HttpRequest& request)
{
    string sql = request.get_option("sql");

    // TODO: Add possiblity to parse using specific parser.
    GWBUF stmt = mariadb::create_query(sql);
    json_t* json = MariaDBParser::get().parse_to_resource(request.host(), stmt).release();

    return HttpResponse(MHD_HTTP_OK, json);
}

HttpResponse cb_qc_cache(const HttpRequest& request)
{
    const int top = 20;
    int val = atoi(request.get_option("top").c_str());
    json_t* json = mxs::CachingParser::content_as_resource(request.host(), val > 0 ? val : top).release();
    return HttpResponse(MHD_HTTP_OK, json);
}

HttpResponse cb_thread(const HttpRequest& request)
{
    int id = atoi(request.last_uri_part().c_str());
    return HttpResponse(MHD_HTTP_OK, mxs_rworker_to_json(request.host(), id));
}

HttpResponse thread_set_listen_mode(const HttpRequest& request, bool enabled)
{
    int id = atoi(request.uri_part(3).c_str());

    if (mxs::RoutingWorker::set_listen_mode(id, enabled))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }
    else
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }
}

HttpResponse cb_thread_listen(const HttpRequest& request)
{
    return thread_set_listen_mode(request, true);
}

HttpResponse cb_thread_unlisten(const HttpRequest& request)
{
    return thread_set_listen_mode(request, false);
}

HttpResponse cb_termination_in_process(const HttpRequest& request)
{
    mxb::Json body;

    body.set_bool("termination_in_process", mxs::RoutingWorker::termination_in_process());

    return HttpResponse(MHD_HTTP_OK, body.release());
}

HttpResponse cb_all_modules(const HttpRequest& request)
{
    static bool all_modules_loaded = false;

    if (!all_modules_loaded && request.get_option("load") == "all")
    {
        if (!load_all_modules())
        {
            return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
        }

        all_modules_loaded = true;
    }

    return HttpResponse(MHD_HTTP_OK, module_list_to_json(request.host()));
}

HttpResponse cb_module(const HttpRequest& request)
{
    json_t* json;

    if (request.last_uri_part() == mxs::Config::get().specification().module())
    {
        json = spec_module_to_json(request.host(), mxs::Config::get().specification());
    }
    else if (request.last_uri_part() == Server::specification().module())
    {
        json = spec_module_to_json(request.host(), Server::specification());
    }
    else
    {
        const MXS_MODULE* module = get_module(request.last_uri_part(), mxs::ModuleType::UNKNOWN);

        json = module_to_json(module, request.host());
    }

    return HttpResponse(MHD_HTTP_OK, json);
}

HttpResponse cb_memory(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, mxs::RoutingWorker::memory_to_json(request.host()).release());
}

HttpResponse cb_all_users(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, admin_all_users_to_json(request.host()));
}

HttpResponse cb_all_inet_users(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, admin_all_users_to_json(request.host()));
}

HttpResponse cb_all_unix_users(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK,
                        mxs_json_resource(request.host(), MXS_JSON_API_USERS "unix", json_array()));
}

HttpResponse cb_inet_user(const HttpRequest& request)
{
    string user = request.uri_part(2);
    return HttpResponse(MHD_HTTP_OK, admin_user_to_json(request.host(), user.c_str()));
}

HttpResponse cb_monitor_wait(const HttpRequest& request)
{
    if (MonitorManager::wait_one_tick(10s))
    {
        return HttpResponse(MHD_HTTP_OK);
    }
    else
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, mxs_json_error("monitor_wait timed out"));
    }
}

HttpResponse cb_profile_snapshot(const HttpRequest& request)
{
    return HttpResponse(MHD_HTTP_OK, mxs::Profiler::get().snapshot(request.host()));
}

HttpResponse cb_debug_server_diagnostics(const HttpRequest& request)
{
    auto servers = MonitorManager::get_connection_settings();
    std::string host = request.host();

    // The server diagnostics requires blocking communication with the databases. To prevent it from blocking
    // the REST-API, the MainWorker and the monitors, they need to be executed asynchronously in the thread
    // pool.
    return HttpResponse([servers, host](){
        return HttpResponse(MHD_HTTP_OK, MonitorManager::server_diagnostics(servers, host.c_str()));
    });
}

HttpResponse cb_create_user(const HttpRequest& request)
{
    mxb_assert(request.get_json());

    if (runtime_create_user_from_json(request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_sql_connect(const HttpRequest& request)
{
    mxb_assert(request.get_json());
    return HttpSql::connect(request);
}

HttpResponse cb_sql_reconnect(const HttpRequest& request)
{
    return HttpSql::reconnect(request);
}

HttpResponse cb_sql_clone(const HttpRequest& request)
{
    return HttpSql::clone(request);
}

HttpResponse cb_sql_get_one(const HttpRequest& request)
{
    return HttpSql::show_connection(request);
}

HttpResponse cb_sql_query_result(const HttpRequest& request)
{
    return HttpSql::query_result(request);
}

HttpResponse cb_sql_get_odbc_drivers(const HttpRequest& request)
{
    return HttpSql::odbc_drivers(request);
}

HttpResponse cb_sql_get_all(const HttpRequest& request)
{
    return HttpSql::show_all_connections(request);
}

HttpResponse cb_sql_disconnect(const HttpRequest& request)
{
    return HttpSql::disconnect(request);
}

HttpResponse cb_sql_cancel(const HttpRequest& request)
{
    return HttpSql::cancel(request);
}

HttpResponse cb_sql_erase_query_result(const HttpRequest& request)
{
    return HttpSql::erase_query_result(request);
}

HttpResponse cb_sql_query(const HttpRequest& request)
{
    mxb_assert(request.get_json());
    return HttpSql::query(request);
}

HttpResponse cb_sql_etl_prepare(const HttpRequest& request)
{
    mxb_assert(request.get_json());
    return HttpSql::etl_prepare(request);
}

HttpResponse cb_sql_etl_start(const HttpRequest& request)
{
    mxb_assert(request.get_json());
    return HttpSql::etl_start(request);
}

HttpResponse cb_alter_user(const HttpRequest& request)
{
    auto user = request.last_uri_part();
    auto type = request.uri_part(1);

    if (runtime_alter_user(user, type, request.get_json()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_alter_session(const HttpRequest& request)
{
    uint64_t id = to_session_id(request.uri_part(1));
    bool ok = false;

    bool found = mxs::RoutingWorker::execute_for_session(id, [&](MXS_SESSION* session){
        mxb::LogRedirect redirect(log_redirect);
        ok = static_cast<Session*>(session)->update(request.get_json());
    });

    if (!found)
    {
        return HttpResponse(MHD_HTTP_NOT_FOUND);
    }

    return ok ? HttpResponse(MHD_HTTP_OK) : HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_restart_session(const HttpRequest& request)
{
    uint64_t id = to_session_id(request.uri_part(1));
    bool ok = false;

    bool found = mxs::RoutingWorker::execute_for_session(id, [&](MXS_SESSION* session){
        mxb::LogRedirect redirect(log_redirect);
        ok = static_cast<Session*>(session)->restart();
    });

    if (!found)
    {
        return HttpResponse(MHD_HTTP_NOT_FOUND);
    }

    return ok ? HttpResponse(MHD_HTTP_OK) : HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_restart_all_sessions(const HttpRequest& request)
{
    bool ok = true;
    mxs::RoutingWorker::execute_concurrently([&](){
        mxb::LogRedirect redirect(log_redirect);

        for (auto [id, session] : mxs::RoutingWorker::get_current()->session_registry())
        {
            if (!static_cast<Session*>(session)->restart())
            {
                ok = false;
            }
        }
    });

    if (!ok)
    {
        config_runtime_add_error("Could not restart all sessions");
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }

    return HttpResponse(MHD_HTTP_OK);
}

HttpResponse cb_delete_session(const HttpRequest& request)
{
    int ttl = atoi(request.get_option("ttl").c_str());
    uint64_t id = to_session_id(request.uri_part(1));

    bool found = mxs::RoutingWorker::execute_for_session(id, [&](MXS_SESSION* session){
        if (ttl > 0)
        {
            static_cast<Session*>(session)->set_ttl(ttl);
        }
        else
        {
            session->kill();
        }
    });

    return found ? HttpResponse(MHD_HTTP_OK) : HttpResponse(MHD_HTTP_NOT_FOUND);
}

HttpResponse cb_delete_user(const HttpRequest& request)
{
    string user = request.last_uri_part();
    string type = request.uri_part(1);

    if (type == CN_INET && runtime_remove_user(user.c_str()))
    {
        return HttpResponse(MHD_HTTP_NO_CONTENT);
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
}

HttpResponse cb_set_server(const HttpRequest& request)
{
    SERVER* server = ServerManager::find_by_unique_name(request.uri_part(1));
    int opt = Server::status_from_string(request.get_option(CN_STATE).c_str());

    if (opt)
    {
        string errmsg;
        if (MonitorManager::set_server_status(server, opt, &errmsg))
        {
            if (status_is_in_maint(opt) && request.is_truthy_option(CN_FORCE))
            {
                BackendDCB::generate_hangup(server, "Server was forced into maintenance");
            }

            return HttpResponse(MHD_HTTP_NO_CONTENT);
        }
        else
        {
            return HttpResponse(MHD_HTTP_BAD_REQUEST, mxs_json_error("%s", errmsg.c_str()));
        }
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST,
                        mxs_json_error("Invalid or missing value for the `%s` parameter", CN_STATE));
}

HttpResponse cb_clear_server(const HttpRequest& request)
{
    SERVER* server = ServerManager::find_by_unique_name(request.uri_part(1));
    int opt = Server::status_from_string(request.get_option(CN_STATE).c_str());

    if (opt)
    {
        string errmsg;
        if (MonitorManager::clear_server_status(server, opt, &errmsg))
        {
            return HttpResponse(MHD_HTTP_NO_CONTENT);
        }
        else
        {
            return HttpResponse(MHD_HTTP_BAD_REQUEST, mxs_json_error("%s", errmsg.c_str()));
        }
    }

    return HttpResponse(MHD_HTTP_BAD_REQUEST,
                        mxs_json_error("Invalid or missing value for the `%s` parameter", CN_STATE));
}

HttpResponse cb_modulecmd(const HttpRequest& request)
{
    std::string module = request.uri_part(2);

    // TODO: If the core ever has module commands, they need to be handled here.
    std::string identifier = request.uri_segment(3, request.uri_part_count());
    std::string verb = request.get_verb();

    const MODULECMD* cmd = modulecmd_find_command(module.c_str(), identifier.c_str());

    if (cmd)
    {
        if ((!MODULECMD_MODIFIES_DATA(cmd) && verb == MHD_HTTP_METHOD_GET)
            || (MODULECMD_MODIFIES_DATA(cmd) && verb == MHD_HTTP_METHOD_POST))
        {
            int n_opts = (int)request.get_option_count();
            std::vector<char*> opts(n_opts);
            request.copy_options(opts.data());

            MODULECMD_ARG* args = modulecmd_arg_parse(cmd, n_opts, (const void**)opts.data());
            bool rval = false;
            json_t* output = NULL;

            if (args)
            {
                rval = modulecmd_call_command(cmd, args, &output);
                modulecmd_arg_free(args);
            }

            for (int i = 0; i < n_opts; i++)
            {
                MXB_FREE(opts[i]);
            }

            int rc;

            if (output)
            {
                /**
                 * Store the command output in the meta field. This allows
                 * all the commands to conform to the JSON API even though
                 * the content of the field can vary from command to command.
                 *
                 * If the output is an JSON API error, we don't do anything to it
                 */
                std::string self = "/";     // The uri_segment doesn't have the leading slash
                self += request.uri_segment(0, request.uri_part_count());
                output = mxs_json_metadata(request.host(), self.c_str(), output);
            }

            if (rval)
            {
                rc = output ? MHD_HTTP_OK : MHD_HTTP_NO_CONTENT;
            }
            else
            {
                rc = MHD_HTTP_BAD_REQUEST;

                json_t* err = runtime_get_json_error(); // {errors: [{detail: "..."}, {...}]}

                if (err)
                {
                    if (!output)
                    {
                        // No output, only errors
                        output = err;
                    }
                    else
                    {
                        // Both output and errors
                        json_object_set(output, CN_ERRORS, json_object_get(err, CN_ERRORS));
                        json_decref(err);
                    }
                }
            }

            return HttpResponse(rc, output);
        }
    }

    return HttpResponse(MHD_HTTP_NOT_FOUND,
                        mxs_json_error("Module '%s' has no command named '%s'.",
                                       module.c_str(), identifier.c_str()));
}

HttpResponse cb_send_ok(const HttpRequest& request)
{
    mxs_rworker_watchdog();
    return HttpResponse(MHD_HTTP_OK);
}

class RootResource
{
    RootResource(const RootResource&);
    RootResource& operator=(const RootResource&);
public:
    using ResourceList = std::vector<Resource>;

    /**
     * Create REST API resources
     *
     * Each resource represents either a collection of resources, an individual
     * resource, a sub-resource of a resource or an "action" endpoint which
     * executes an action.
     *
     * The resources are defined by the Resource class. Each resource maps to a
     * HTTP method and one or more paths. The path components can contain either
     * an explicit string, a colon-prefixed object type or a question mark for
     * a path component that matches everything.
     */
    RootResource()
    {
        const auto REQ_BODY = Resource::REQUIRE_BODY;
        const auto REQ_SYNC = Resource::REQUIRE_SYNC;

        // Special resources required by OPTION etc.
        m_get.emplace_back(cb_send_ok);
        m_get.emplace_back(cb_send_ok, "*");

        m_get.emplace_back(cb_all_servers, "servers");
        m_get.emplace_back(cb_get_server, "servers", ":server");

        m_get.emplace_back(cb_all_services, "services");
        m_get.emplace_back(cb_get_service, "services", ":service");
        m_get.emplace_back(cb_get_all_service_listeners, "services", ":service", "listeners");
        m_get.emplace_back(cb_get_service_listener, "services", ":service", "listeners", ":listener");

        m_get.emplace_back(cb_get_all_listeners, "listeners");
        m_get.emplace_back(cb_get_listener, "listeners", ":listener");

        m_get.emplace_back(cb_all_filters, "filters");
        m_get.emplace_back(cb_get_filter, "filters", ":filter");

        m_get.emplace_back(cb_all_monitors, "monitors");
        m_get.emplace_back(cb_get_monitor, "monitors", ":monitor");

        m_get.emplace_back(cb_all_sessions, "sessions");
        m_get.emplace_back(cb_get_session, "sessions", ":session");

        /** Get resource relationships directly */
        m_get.emplace_back(cb_get_server_service_relationship,
                           "servers", ":server", "relationships", "services");
        m_get.emplace_back(cb_get_server_monitor_relationship,
                           "servers", ":server", "relationships", "monitors");
        m_get.emplace_back(cb_get_monitor_server_relationship,
                           "monitors", ":monitor", "relationships", "servers");
        m_get.emplace_back(cb_get_monitor_service_relationship,
                           "monitors", ":monitor", "relationships", "services");
        m_get.emplace_back(cb_get_service_server_relationship,
                           "services", ":service", "relationships", "servers");
        m_get.emplace_back(cb_get_service_service_relationship,
                           "services", ":service", "relationships", "services");
        m_get.emplace_back(cb_get_service_filter_relationship,
                           "services", ":service", "relationships", "filters");
        m_get.emplace_back(cb_get_service_monitor_relationship,
                           "services", ":service", "relationships", "monitors");
        m_get.emplace_back(cb_get_service_listener_relationship,
                           "services", ":service", "relationships", "listeners");
        m_get.emplace_back(cb_get_filter_service_relationship,
                           "filters", ":filter", "relationships", "services");
        m_get.emplace_back(cb_get_listener_service_relationship,
                           "listeners", ":listener", "relationships", "services");

        m_get.emplace_back(cb_maxscale, "maxscale");
        m_get.emplace_back(cb_qc, "maxscale", "query_classifier");
        m_get.emplace_back(cb_qc_classify, "maxscale", "query_classifier", "classify");
        m_get.emplace_back(cb_qc_cache, "maxscale", "query_classifier", "cache");
        m_get.emplace_back(cb_all_threads, "maxscale", "threads");
        m_get.emplace_back(cb_thread, "maxscale", "threads", ":thread");
        m_get.emplace_back(cb_logs, "maxscale", "logs");
        m_get.emplace_back(cb_log_data, "maxscale", "logs", "data");
        m_get.emplace_back(cb_log_entries, "maxscale", "logs", "entries");
        m_get.emplace_back(cb_log_stream, "maxscale", "logs", "stream");
        m_get.emplace_back(cb_all_modules, "maxscale", "modules");
        m_get.emplace_back(cb_module, "maxscale", "modules", ":module");
        m_get.emplace_back(cb_memory, "maxscale", "memory");

        /** For all read-only module commands */
        m_get.emplace_back(cb_modulecmd, "maxscale", "modules", ":module", "?");

        m_get.emplace_back(cb_all_users, "users");
        m_get.emplace_back(cb_all_inet_users, "users", "inet");
        m_get.emplace_back(cb_all_unix_users, "users", "unix");     // For backward compatibility.
        m_get.emplace_back(cb_inet_user, "users", "inet", ":inetuser");

        /** SQL connection inspection endpoints */
        m_get.emplace_back(cb_sql_get_all, "sql");
        m_get.emplace_back(cb_sql_get_one, "sql", ":connection_id");
        m_get.emplace_back(cb_sql_query_result, "sql", ":connection_id", "queries", ":query_id");
        m_get.emplace_back(cb_sql_get_odbc_drivers, "sql", "odbc", "drivers");

        /** Debug utility endpoints */
        m_get.emplace_back(cb_monitor_wait, "maxscale", "debug", "monitor_wait");
        m_put.emplace_back(cb_thread_listen, "maxscale", "debug", "threads", ":thread", "listen");
        m_put.emplace_back(cb_thread_unlisten, "maxscale", "debug", "threads", ":thread", "unlisten");
        m_get.emplace_back(cb_termination_in_process, "maxscale", "debug", "termination_in_process");
        m_get.emplace_back(cb_profile_snapshot, "maxscale", "debug", "stacktrace");
        m_get.emplace_back(cb_debug_server_diagnostics, "maxscale", "debug", "server_diagnostics");

        /** Create new resources */
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_server, "servers");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_monitor, "monitors");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_filter, "filters");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_service, "services");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_service_listener,
                            "services", ":service", "listeners");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_listener, "listeners");
        m_post.emplace_back(REQ_BODY | REQ_SYNC, cb_create_user, "users", "inet");
        // For backward compatibility.
        m_post.emplace_back(REQ_BODY, cb_create_user, "users", "unix");

        /** SQL connection management endpoints */
        m_post.emplace_back(REQ_BODY, cb_sql_connect, "sql");
        m_post.emplace_back(cb_sql_reconnect, "sql", ":connection_id", "reconnect");
        m_post.emplace_back(cb_sql_clone, "sql", ":connection_id", "clone");
        m_post.emplace_back(cb_sql_cancel, "sql", ":connection_id", "cancel");
        m_post.emplace_back(REQ_BODY, cb_sql_query, "sql", ":connection_id", "queries");
        m_post.emplace_back(REQ_BODY, cb_sql_etl_prepare, "sql", ":connection_id", "etl", "prepare");
        m_post.emplace_back(REQ_BODY, cb_sql_etl_start, "sql", ":connection_id", "etl", "start");

        /** For all module commands that modify state/data */
        m_post.emplace_back(cb_modulecmd, "maxscale", "modules", ":module", "?");
        m_post.emplace_back(cb_flush, "maxscale", "logs", "flush");
        m_post.emplace_back(cb_tls_reload, "maxscale", "tls", "reload");
        m_post.emplace_back(cb_thread_rebalance, "maxscale", "threads", ":thread", "rebalance");
        m_post.emplace_back(cb_threads_rebalance, "maxscale", "threads", "rebalance");
        m_post.emplace_back(cb_reload_users, "services", ":service", "reload");

        /** Session manipulation */
        m_post.emplace_back(cb_restart_session, "sessions", ":session", "restart");
        m_post.emplace_back(cb_restart_all_sessions, "sessions", "restart");

        /** Update resources */
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_server, "servers", ":server");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_monitor, "monitors", ":monitor");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_service, "services", ":service");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_filter, "filters", ":filter");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_listener, "listeners", ":listener");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_maxscale, "maxscale", "logs");   // Deprecated
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_maxscale, "maxscale");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_qc, "maxscale", "query_classifier");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_user, "users", "inet", ":inetuser");
        m_patch.emplace_back(REQ_BODY, cb_alter_session, "sessions", ":session");

        /** Update resource relationships directly */
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_server_service_relationship,
                             "servers", ":server", "relationships", "services");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_server_monitor_relationship,
                             "servers", ":server", "relationships", "monitors");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_monitor_server_relationship,
                             "monitors", ":monitor", "relationships", "servers");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_monitor_service_relationship,
                             "monitors", ":monitor", "relationships", "services");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_service_server_relationship,
                             "services", ":service", "relationships", "servers");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_service_service_relationship,
                             "services", ":service", "relationships", "services");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_service_filter_relationship,
                             "services", ":service", "relationships", "filters");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_service_monitor_relationship,
                             "services", ":service", "relationships", "monitors");
        m_patch.emplace_back(REQ_BODY | REQ_SYNC, cb_alter_session_filter_relationship,
                             "sessions", ":session", "relationships", "filters");

        /** Change resource states */
        // TODO: Sync these once object states are synchronized as well
        m_put.emplace_back(cb_stop_monitor, "monitors", ":monitor", "stop");
        m_put.emplace_back(cb_start_monitor, "monitors", ":monitor", "start");
        m_put.emplace_back(cb_stop_service, "services", ":service", "stop");
        m_put.emplace_back(cb_start_service, "services", ":service", "start");
        m_put.emplace_back(cb_stop_listener, "listeners", ":listener", "stop");
        m_put.emplace_back(cb_start_listener, "listeners", ":listener", "start");
        m_put.emplace_back(REQ_SYNC, cb_set_server, "servers", ":server", "set");
        m_put.emplace_back(REQ_SYNC, cb_clear_server, "servers", ":server", "clear");

        m_delete.emplace_back(REQ_SYNC, cb_delete_server, "servers", ":server");
        m_delete.emplace_back(REQ_SYNC, cb_delete_monitor, "monitors", ":monitor");
        m_delete.emplace_back(REQ_SYNC, cb_delete_service, "services", ":service");
        m_delete.emplace_back(REQ_SYNC, cb_delete_filter, "filters", ":filter");
        m_delete.emplace_back(REQ_SYNC, cb_delete_listener, "listeners", ":listener");
        m_delete.emplace_back(REQ_SYNC, cb_delete_service_listener,
                              "services", ":service", "listeners", ":listener");

        m_delete.emplace_back(REQ_SYNC, cb_delete_user, "users", "inet", ":inetuser");
        m_delete.emplace_back(cb_delete_session, "sessions", ":session");

        /** SQL connection destruction */
        m_delete.emplace_back(cb_sql_disconnect, "sql", ":connection_id");
        m_delete.emplace_back(cb_sql_erase_query_result, "sql", ":connection_id", "queries", ":query_id");

        std::sort(m_get.begin(), m_get.end());
        std::sort(m_put.begin(), m_put.end());
        std::sort(m_post.begin(), m_post.end());
        std::sort(m_delete.begin(), m_delete.end());
        std::sort(m_patch.begin(), m_patch.end());
    }

    ~RootResource()
    {
    }

    template<class ResIter, class PathIter>
    const std::pair<ResIter, ResIter> find_matching_resources(ResIter rbeg, ResIter rend,
                                                              PathIter pbeg, PathIter pend,
                                                              size_t depth) const
    {
        auto it = rbeg;

        if (it == rend || pbeg == pend)
        {
            // No match or we ran out of path parts
            return {it, rend};
        }

        while (it != rend && !it->part_matches(*pbeg, depth))
        {
            ++it;
        }

        if (it == rend)
        {
            // Nothing matches, return the whole range that matched in the previous search step
            return {rbeg, rend};
        }

        auto it_start = it;

        while (it != rend && it->part_matches(*pbeg, depth))
        {
            ++it;
        }

        return find_matching_resources(it_start, it, pbeg + 1, pend, depth + 1);
    }

    const Resource* find_resource(const ResourceList& list, const HttpRequest& request) const
    {
        const auto& parts = request.uri_parts();
        auto [start, end] = find_matching_resources(list.begin(), list.end(), parts.begin(), parts.end(), 0);

        for (auto it = start; it != end; ++it)
        {
            if (it->match(request))
            {
                return &(*it);
            }
        }

        if (start != list.begin() || end != list.end())
        {
            // Some part of the path matched some resources. Try to figure out if any of them is a name of an
            // object and report it as a name mismatch. The error messages will be redirected to the REST-API
            // clients.
            for (auto it = start; it != end; ++it)
            {
                const auto& path = it->path();

                if (it->variable_part_mismatch(parts))
                {
                    for (size_t i = 0; i < path.size(); i++)
                    {
                        if (!it->part_matches(parts[i], i))
                        {
                            if (path[i][0] == ':')
                            {
                                MXB_ERROR("%s is not a %s", parts[i].c_str(), path[i].c_str() + 1);
                            }

                            break;
                        }
                    }
                }
            }
        }

        return nullptr;
    }

    const Resource* find_resource(const HttpRequest& request) const
    {
        if (request.get_verb() == MHD_HTTP_METHOD_GET)
        {
            return find_resource(m_get, request);
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_PUT)
        {
            return find_resource(m_put, request);
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_PATCH)
        {
            return find_resource(m_patch, request);
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_POST)
        {
            return find_resource(m_post, request);
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_DELETE)
        {
            return find_resource(m_delete, request);
        }

        return nullptr;
    }

    HttpResponse process_request_type(const ResourceList& list, const HttpRequest& request)
    {
        if (const auto* res = find_resource(list, request))
        {
            return res->call(request);
        }

        return HttpResponse(MHD_HTTP_NOT_FOUND);
    }

    string get_supported_methods(const HttpRequest& request)
    {
        std::vector<string> l;

        if (find_resource(m_get, request))
        {
            l.push_back(MHD_HTTP_METHOD_GET);
        }
        if (find_resource(m_put, request))
        {
            l.push_back(MHD_HTTP_METHOD_PUT);
        }
        if (find_resource(m_post, request))
        {
            l.push_back(MHD_HTTP_METHOD_POST);
        }
        if (find_resource(m_delete, request))
        {
            l.push_back(MHD_HTTP_METHOD_DELETE);
        }

        return mxb::join(l, ", ");
    }

    HttpResponse process_request(const HttpRequest& request, const Resource* resource)
    {
        HttpResponse response(MHD_HTTP_NOT_FOUND, runtime_get_json_error());

        if (resource)
        {
            response = resource->call(request);
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_OPTIONS)
        {
            string methods = get_supported_methods(request);

            if (!methods.empty())
            {
                response.set_code(MHD_HTTP_OK);
                response.add_header(HTTP_RESPONSE_HEADER_ACCEPT, methods);
            }
        }
        else if (request.get_verb() == MHD_HTTP_METHOD_HEAD)
        {
            /** Do a GET and just drop the body of the response */
            if (const auto* res = find_resource(m_get, request))
            {
                response = res->call(request);
                response.drop_response();
            }
        }

        return response;
    }

private:

    ResourceList m_get;     /**< GET request handlers */
    ResourceList m_put;     /**< PUT request handlers */
    ResourceList m_post;    /**< POST request handlers */
    ResourceList m_delete;  /**< DELETE request handlers */
    ResourceList m_patch;   /**< PATCH request handlers */
};

struct ThisUnit
{
    RootResource    resources;          /**< Core resource set */
    ResourceWatcher watcher;            /**< Modification watcher */
};

ThisUnit this_unit;

static bool is_unknown_method(const std::string& verb)
{
    static std::unordered_set<std::string> supported_methods
    {
        MHD_HTTP_METHOD_GET,
        MHD_HTTP_METHOD_PUT,
        MHD_HTTP_METHOD_PATCH,
        MHD_HTTP_METHOD_POST,
        MHD_HTTP_METHOD_DELETE,
        MHD_HTTP_METHOD_OPTIONS,
        MHD_HTTP_METHOD_HEAD
    };

    return supported_methods.find(verb) == supported_methods.end();
}

static bool request_modifies_data(const string& verb)
{
    return verb == MHD_HTTP_METHOD_POST
           || verb == MHD_HTTP_METHOD_PUT
           || verb == MHD_HTTP_METHOD_DELETE
           || verb == MHD_HTTP_METHOD_PATCH;
}

static bool request_reads_data(const string& verb)
{
    return verb == MHD_HTTP_METHOD_GET
           || verb == MHD_HTTP_METHOD_HEAD;
}

static bool request_precondition_met(const HttpRequest& request, HttpResponse& response,
                                     const std::string& cksum)
{
    bool rval = false;
    const string& uri = request.get_uri();
    auto if_modified_since = request.get_header(MHD_HTTP_HEADER_IF_MODIFIED_SINCE);
    auto if_unmodified_since = request.get_header(MHD_HTTP_HEADER_IF_UNMODIFIED_SINCE);
    auto if_match = request.get_header(MHD_HTTP_HEADER_IF_MATCH);
    auto if_none_match = request.get_header(MHD_HTTP_HEADER_IF_NONE_MATCH);

    if ((!if_unmodified_since.empty()
         && this_unit.watcher.last_modified(uri) > http_from_date(if_unmodified_since))
        || (!if_match.empty() && cksum != if_match))
    {
        response = HttpResponse(MHD_HTTP_PRECONDITION_FAILED);
    }
    else if (!if_modified_since.empty() || !if_none_match.empty())
    {
        if ((if_modified_since.empty()
             || this_unit.watcher.last_modified(uri) <= http_from_date(if_modified_since))
            && (if_none_match.empty() || cksum == if_none_match))
        {
            response = HttpResponse(MHD_HTTP_NOT_MODIFIED);
        }
    }
    else
    {
        rval = true;
    }

    return rval;
}

static void remove_unwanted_fields(const HttpRequest& request, HttpResponse& response)
{
    for (const auto& a : request.get_options())
    {
        const char FIELDS[] = "fields[";
        auto s = a.first.substr(0, sizeof(FIELDS) - 1);

        if (s == FIELDS && a.first.back() == ']')
        {
            auto type = a.first.substr(s.size(), a.first.size() - s.size() - 1);
            auto fields = mxb::strtok(a.second, ",");

            if (!fields.empty())
            {
                response.remove_fields(type, {fields.begin(), fields.end()});
            }
        }
    }
}

static bool remove_unwanted_rows(const HttpRequest& request, HttpResponse& response)
{
    bool ok = true;
    const std::string FILTER = "filter";
    const std::string FILTER_PATH = "filter[";
    const auto& options = request.get_options();

    if (auto it = options.find(FILTER); it != options.end())
    {
        const auto& filter = it->second;
        auto pos = filter.find('=');
        if (pos != std::string::npos)
        {
            auto json_ptr = filter.substr(0, pos);
            auto value = filter.substr(pos + 1);
            ok = response.remove_rows(json_ptr, value);
        }
        else
        {
            MXB_ERROR("Invalid filter expression: %s", filter.c_str());
        }
    }

    // Handle the filtering that uses JSON Path values of the form filter[PATH]=EXPR
    for (const auto& [key, value] : options)
    {
        if (key.find(FILTER_PATH) == 0 && key.back() == ']')
        {
            auto path = key.substr(FILTER_PATH.size(), key.size() - FILTER_PATH.size() - 1);
            ok = response.remove_rows_json_path(path, value);

            if (!ok)
            {
                break;
            }
        }
    }

    return ok;
}

static void paginate_result(const HttpRequest& request, HttpResponse& response)
{
    auto limit = request.get_option("page[size]");
    auto offset = request.get_option("page[number]");

    if (!limit.empty())
    {
        int64_t lim = strtol(limit.c_str(), nullptr, 10);
        int64_t off = offset.empty() ? 0 :  strtol(offset.c_str(), nullptr, 10);

        if (lim > 0 && off >= 0)
        {
            response.paginate(lim, off);
        }
    }
}

static HttpResponse handle_request(const HttpRequest& request)
{
    // Redirect log output into the runtime error message buffer
    mxb::LogRedirect redirect(log_redirect);

    MXB_DEBUG("%s %s %s",
              request.get_verb().c_str(),
              request.get_uri().c_str(),
              request.get_json_str().c_str());

    const Resource* resource = this_unit.resources.find_resource(request);
    bool modifies_data = request_modifies_data(request.get_verb());
    bool requires_sync = false;
    bool skip_sync = request.is_falsy_option("sync");

    if (resource)
    {
        requires_sync = resource->requires_sync();

        if (requires_sync && skip_sync)
        {
            MXB_NOTICE("Disabling configuration sync for: %s %s",
                       request.get_verb().c_str(), request.get_uri().c_str());
        }

        if (resource->requires_body() && !request.get_json())
        {
            return HttpResponse(MHD_HTTP_BAD_REQUEST, mxs_json_error("Missing request body"));
        }
    }

    auto manager = mxs::ConfigManager::get();
    mxb_assert(manager);

    if (requires_sync && !skip_sync && !manager->start())
    {
        return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
    }

    HttpResponse rval = this_unit.resources.process_request(request, resource);

    std::string warning = runtime_get_warnings();

    if (!warning.empty())
    {
        rval.add_header("Mxs-Warning", warning);
    }

    // Calculate the checksum from the generated JSON
    auto str = mxb::json_dump(rval.get_response(), JSON_COMPACT);
    auto cksum = '"' + mxb::checksum<mxb::Sha1Sum>(str) + '"';

    if (request_precondition_met(request, rval, cksum))
    {
        if (modifies_data)
        {
            switch (rval.get_code())
            {
            case MHD_HTTP_OK:
            case MHD_HTTP_NO_CONTENT:
            case MHD_HTTP_CREATED:
                this_unit.watcher.modify(request.get_uri());

                if (requires_sync)
                {
                    if (skip_sync)
                    {
                        // No synchronization, just update the JSON representation of the configuration
                        manager->refresh();
                    }
                    else if (!manager->commit())
                    {
                        rval = HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
                    }
                }
                break;

            default:
                if (requires_sync && !skip_sync)
                {
                    manager->rollback();
                }
                break;
            }
        }
        else if (request_reads_data(request.get_verb()))
        {
            const auto& uri = request.get_uri();
            rval.add_header(HTTP_RESPONSE_HEADER_LAST_MODIFIED,
                            http_to_date(this_unit.watcher.last_modified(uri)));
            rval.add_header(HTTP_RESPONSE_HEADER_ETAG, cksum.c_str());
        }

        if (!remove_unwanted_rows(request, rval))
        {
            return HttpResponse(MHD_HTTP_BAD_REQUEST, runtime_get_json_error());
        }

        paginate_result(request, rval);
        remove_unwanted_fields(request, rval);
    }

    return rval;
}
}

HttpResponse resource_handle_request(const HttpRequest& request)
{
    mxb::WatchedWorker* worker = mxs::MainWorker::get();
    HttpResponse response;

    if (is_unknown_method(request.get_verb()))
    {
        return HttpResponse(MHD_HTTP_METHOD_NOT_ALLOWED);
    }

    if (!worker->call([&request, &response, worker]() {
                          mxb::WatchdogNotifier::Workaround workaround(worker);
                          response = handle_request(request);
                      }))
    {
        response = HttpResponse(MHD_HTTP_SERVICE_UNAVAILABLE);
    }

    return response;
}
