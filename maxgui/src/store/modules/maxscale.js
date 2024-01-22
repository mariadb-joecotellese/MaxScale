/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
import { APP_CONFIG } from '@rootSrc/utils/constants'
import { t } from 'typy'

const PAGE_CURSOR_REG = /page\[cursor\]=([^&]+)/
function getPageCursorParam(url) {
    return t(url.match(PAGE_CURSOR_REG), '[0]').safeString
}
export default {
    namespaced: true,
    state: {
        all_obj_ids: [],
        maxscale_version: '',
        maxscale_overview_info: {},
        all_modules_map: {},
        thread_stats: [],
        threads_datasets: [],
        maxscale_parameters: {},
        config_sync: null,
        logs_page_size: 100,
        latest_logs: [],
        prev_log_link: null,
        prev_logs: [],
        log_source: null,
        hidden_log_levels: [],
        log_date_range: [],
    },
    mutations: {
        SET_ALL_OBJ_IDS(state, payload) {
            state.all_obj_ids = payload
        },
        SET_MAXSCALE_VERSION(state, payload) {
            state.maxscale_version = payload
        },
        SET_MAXSCALE_OVERVIEW_INFO(state, payload) {
            state.maxscale_overview_info = payload
        },
        SET_ALL_MODULES_MAP(state, payload) {
            state.all_modules_map = payload
        },
        SET_THREAD_STATS(state, payload) {
            state.thread_stats = payload
        },
        SET_THREADS_DATASETS(state, payload) {
            state.threads_datasets = payload
        },
        SET_MAXSCALE_PARAMETERS(state, payload) {
            state.maxscale_parameters = payload
        },
        SET_CONFIG_SYNC(state, payload) {
            state.config_sync = payload
        },
        SET_LATEST_LOGS(state, payload) {
            state.latest_logs = payload
        },
        SET_PREV_LOG_LINK(state, payload) {
            state.prev_log_link = payload
        },
        SET_LOG_SOURCE(state, payload) {
            state.log_source = payload
        },
        SET_PREV_LOGS(state, payload) {
            state.prev_logs = payload
        },
        SET_HIDDEN_LOG_LEVELS(state, payload) {
            state.hidden_log_levels = payload
        },
        SET_LOG_DATE_RANGE(state, payload) {
            state.log_date_range = payload
        },
    },
    actions: {
        async fetchMaxScaleParameters({ commit }) {
            try {
                let res = await this.vue.$http.get(`/maxscale?fields[maxscale]=parameters`)
                if (res.data.data.attributes.parameters)
                    commit('SET_MAXSCALE_PARAMETERS', res.data.data.attributes.parameters)
            } catch (e) {
                this.vue.$logger.error(e)
            }
        },
        async fetchConfigSync({ commit }) {
            const [, res] = await this.vue.$helpers.to(
                this.vue.$http.get(`/maxscale?fields[maxscale]=config_sync`)
            )
            commit(
                'SET_CONFIG_SYNC',
                this.vue.$typy(res, 'data.data.attributes.config_sync').safeObject
            )
        },
        async fetchMaxScaleOverviewInfo({ commit }) {
            try {
                let res = await this.vue.$http.get(
                    `/maxscale?fields[maxscale]=version,commit,started_at,activated_at,uptime`
                )
                if (res.data.data.attributes)
                    commit('SET_MAXSCALE_OVERVIEW_INFO', res.data.data.attributes)
            } catch (e) {
                this.vue.$logger.error(e)
            }
        },
        async fetchAllModules({ commit }) {
            try {
                let res = await this.vue.$http.get(`/maxscale/modules?load=all`)
                if (res.data.data) {
                    const allModules = res.data.data

                    let hashMap = this.vue.$helpers.hashMapByPath({
                        arr: allModules,
                        path: 'attributes.module_type',
                    })

                    commit('SET_ALL_MODULES_MAP', hashMap)
                }
            } catch (e) {
                this.vue.$logger.error(e)
            }
        },

        async fetchThreadStats({ commit }) {
            try {
                let res = await this.vue.$http.get(`/maxscale/threads?fields[threads]=stats`)
                if (res.data.data) commit('SET_THREAD_STATS', res.data.data)
            } catch (e) {
                this.vue.$logger.error(e)
            }
        },

        genDataSets({ commit, state }) {
            const { thread_stats } = state
            const { genLineStreamDataset } = this.vue.$helpers
            if (thread_stats.length) {
                let dataSets = []
                thread_stats.forEach((thread, i) => {
                    const {
                        attributes: { stats: { load: { last_second = null } = {} } = {} } = {},
                    } = thread
                    if (last_second !== null) {
                        const dataset = genLineStreamDataset({
                            label: `THREAD ID - ${thread.id}`,
                            value: last_second,
                            colorIndex: i,
                        })
                        dataSets.push(dataset)
                    }
                })
                commit('SET_THREADS_DATASETS', dataSets)
            }
        },
        async fetchLatestLogs({ commit, getters }) {
            const [, res] = await this.vue.$helpers.to(
                this.vue.$http.get(`/maxscale/logs/entries?${getters.logsParams}`)
            )
            const { data = [], links: { prev = '' } = {} } = res.data
            commit('SET_LATEST_LOGS', Object.freeze(data))
            const logSource = this.vue.$typy(data, '[0].attributes.log_source').safeString
            if (logSource) commit('SET_LOG_SOURCE', logSource)
            commit('SET_PREV_LOG_LINK', prev)
        },
        async fetchPrevLogs({ commit, getters }) {
            const [, res] = await this.vue.$helpers.to(
                this.vue.$http.get(`/maxscale/logs/entries?${getters.prevLogsParams}`)
            )
            const {
                data,
                links: { prev = '' },
            } = res.data
            commit('SET_PREV_LOGS', Object.freeze(data))
            commit('SET_PREV_LOG_LINK', prev)
        },
        //-----------------------------------------------Maxscale parameter update---------------------------------
        /**
         * @param {Object} payload payload object
         * @param {String} payload.id maxscale
         * @param {Object} payload.parameters Parameters for the monitor
         * @param {Object} payload.callback callback function after successfully updated
         */
        async updateMaxScaleParameters({ commit }, payload) {
            try {
                const body = {
                    data: {
                        id: payload.id,
                        type: 'maxscale',
                        attributes: { parameters: payload.parameters },
                    },
                }
                let res = await this.vue.$http.patch(`/maxscale`, body)
                // response ok
                if (res.status === 204) {
                    commit(
                        'mxsApp/SET_SNACK_BAR_MESSAGE',
                        {
                            text: [`MaxScale parameters is updated`],
                            type: 'success',
                        },
                        { root: true }
                    )
                    await this.vue.$typy(payload.callback).safeFunction()
                }
            } catch (e) {
                this.vue.$logger.error(e)
            }
        },
        async fetchAllMxsObjIds({ commit, dispatch }) {
            const types = ['servers', 'monitors', 'filters', 'services', 'listeners']
            let ids = []
            for (const type of types) {
                const data = await dispatch(
                    'getResourceData',
                    { type, fields: ['id'] },
                    { root: true }
                )
                ids.push(...data.map(item => item.id))
            }
            commit('SET_ALL_OBJ_IDS', ids)
        },
    },
    getters: {
        getMxsObjModules: (state, getters, rootState) => objType => {
            const {
                SERVICES,
                SERVERS,
                MONITORS,
                LISTENERS,
                FILTERS,
            } = rootState.app_config.MXS_OBJ_TYPES
            switch (objType) {
                case SERVICES:
                    return t(state.all_modules_map['Router']).safeArray
                case SERVERS:
                    return t(state.all_modules_map['servers']).safeArray
                case MONITORS:
                    return t(state.all_modules_map['Monitor']).safeArray
                case FILTERS:
                    return t(state.all_modules_map['Filter']).safeArray
                case LISTENERS: {
                    let authenticators = t(state.all_modules_map['Authenticator']).safeArray.map(
                        item => item.id
                    )
                    let protocols = t(state.all_modules_map['Protocol']).safeArray || []
                    if (protocols.length) {
                        protocols.forEach(protocol => {
                            protocol.attributes.parameters = protocol.attributes.parameters.filter(
                                o => o.name !== 'protocol' && o.name !== 'service'
                            )
                            // Transform authenticator parameter from string type to enum type,
                            let authenticatorParamObj = protocol.attributes.parameters.find(
                                o => o.name === 'authenticator'
                            )
                            if (authenticatorParamObj) {
                                authenticatorParamObj.type = 'enum'
                                authenticatorParamObj.enum_values = authenticators
                                // add default_value for authenticator
                                authenticatorParamObj.default_value = ''
                            }
                        })
                    }
                    return protocols
                }
                default:
                    return []
            }
        },
        getChosenLogLevels: state =>
            APP_CONFIG.MAXSCALE_LOG_LEVELS.filter(type => !state.hidden_log_levels.includes(type)),
        logPriorityParam: (state, getters) => `priority=${getters.getChosenLogLevels.join(',')}`,
        logDateRangeParam: state => {
            const [from, to] = state.log_date_range
            if (from && to) return `filter=attributes/unix_timestamp=and(ge(${from}),le(${to}))`
            return ''
        },
        logsParams: ({ logs_page_size }, { logPriorityParam, logDateRangeParam }) =>
            `page[size]=${logs_page_size}&${logPriorityParam}&${logDateRangeParam}`,
        prevPageCursorParam: state => getPageCursorParam(decodeURIComponent(state.prev_log_link)),
        prevLogsParams: (state, getters) => `${getters.prevPageCursorParam}&${getters.logsParams}`,
    },
}
