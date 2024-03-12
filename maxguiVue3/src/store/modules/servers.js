/*
 * Copyright (c) 2023 MariaDB plc
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
import { genSetMutations, lodash } from '@/utils/helpers'

const states = () => ({
  all_servers: [],
  obj_data: {},
})

export default {
  namespaced: true,
  state: states(),
  mutations: genSetMutations(states()),
  actions: {
    async fetchAll({ commit }) {
      try {
        let res = await this.vue.$http.get(`/servers`)
        if (res.data.data) commit('SET_ALL_SERVERS', res.data.data)
      } catch (e) {
        this.vue.$logger.error(e)
      }
    },
    /**
     * @param {Object} payload payload object
     * @param {String} payload.id Name of the server
     * @param {Object} payload.parameters Parameters for the server
     * @param {Object} payload.relationships The relationships of the server to other resources
     * @param {Object} payload.relationships.services services object
     * @param {Object} payload.relationships.monitors monitors object
     * @param {Function} payload.callback callback function after successfully updated
     */
    async create({ commit }, payload) {
      try {
        const body = {
          data: {
            id: payload.id,
            type: 'servers',
            attributes: {
              parameters: payload.parameters,
            },
            relationships: payload.relationships,
          },
        }
        let res = await this.vue.$http.post(`/servers/`, body)
        let message = [`Server ${payload.id} is created`]
        // response ok
        if (res.status === 204) {
          commit(
            'mxsApp/SET_SNACK_BAR_MESSAGE',
            {
              text: message,
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
  },
  getters: {
    // -------------- below getters are available only when fetchAll has been dispatched
    total: (state) => state.all_servers.length,
    map: (state) => lodash.keyBy(state.all_servers, 'id'),
    getCurrStateMode: () => {
      return (serverState) => {
        let currentState = serverState.toLowerCase()
        if (currentState.indexOf(',') > 0) {
          currentState = currentState.slice(0, currentState.indexOf(','))
        }
        return currentState
      }
    },
  },
}
